import redis
import msgpack
from copy import deepcopy


class SimpleKV(object):
    """ A simple key value storage based on redis.
    Values are serialized as messagepack.
    Attributes can be then accessed like object attrs, i.e.:

    >>> s = SimpleKV('a')
    >>> s.foo = 'bar'
    >>> s.foo
    'bar'
    """
    def __init__(self, namespace):
        self._obj_setattr('namespace', str(namespace))
        self._obj_setattr('redis_client', redis.StrictRedis())

    def __getattr__(self, item):
        r_client = object.__getattribute__(self, 'redis_client')
        res = r_client.hget(self._redis_ns, item)
        if res is not None:
            return msgpack.loads(res)
        return object.__getattribute__(self, item)

    def __setattr__(self, key, value):
        return self.redis_client.hset(self._redis_ns, key, msgpack.dumps(value))

    def __repr__(self):
        return repr(self.getall())

    def __str__(self):
        return str(self.getall())

    def _obj_setattr(self, name, value):
        object.__setattr__(self, name, value)

    def _obj_getattr(self, name):
        object.__getattribute__(self, name)

    @property
    def _redis_ns(self):
        return '{0}:SimpleKV'.format(self.namespace)

    def getall(self):
        attrs = self.redis_client.hgetall(self._redis_ns)
        return {k: msgpack.loads(v) for k, v in attrs.iteritems()}

    def get(self, key, default=None):
        """ PO.get(key[,d]) -> D[key] if key in PO, else d.
        d defaults to None.
        """
        res = self.redis_client.hget(self._redis_ns, key)
        if res is None:
            return default
        return msgpack.loads(res)

    def getset(self, key, value, default=None):
        """ Sets the value at key ``key`` to ``value``
        and returns the old value at key ``key`` atomically.
        """
        p = self.redis_client.pipeline()
        p.multi()

        p.hget(self._redis_ns, key)
        p.hset(self._redis_ns, key, msgpack.dumps(value))

        res = p.execute()[0]
        if res is None:
            return default

        return msgpack.loads(res)

    def exists(self, key):
        return self.redis_client.hexists(self._redis_ns, key)

    def delete(self):
        return self.redis_client.delete(self._redis_ns)


class PersistentObject(object):
    """
    An easy way to save and restore a bunch of variables wrapping themselves
    into an object and saving/loading the object on/from redis.
    Similar to SimpleKV, but much faster since redis is involved in load / save
    operations only. Not recommended in concurrent environments.
    """
    def __init__(self, namespace, default=None):
        if default is None:
            default = {}
        object.__setattr__(self, 'namespace', namespace)
        object.__setattr__(self, 'attrs', deepcopy(default))
        object.__setattr__(self, 'redis_client', redis.StrictRedis())

        self.load()

    def __getattr__(self, item):
        attrs = object.__getattribute__(self, 'attrs')
        if item in attrs:
            return attrs[item]
        return object.__getattribute__(self, item)

    def __setattr__(self, key, value):
        self.attrs[key] = value

    def __repr__(self):
        return repr(self.attrs)

    def __str__(self):
        return str(self.attrs)

    @property
    def _redis_ns(self):
        """ Generate the redis key for this PersistentObject """
        return '{0}:PersistentObject'.format(self.namespace)

    def save(self):
        """ Save the data on redis """
        self.redis_client.set(self._redis_ns, msgpack.dumps(self.attrs))

    def load(self):
        """ Load the data from redis"""
        serialized = self.redis_client.get(self._redis_ns)
        if serialized is not None:
            stored_val = msgpack.loads(serialized)
            self.attrs.update(stored_val or {})

    def get(self, attr, default=None):
        """ PO.get(k[,d]) -> D[k] if k in PO, else d.  d defaults to None. """
        if attr in self.attrs:
            return self.attrs[attr]
        return default

    def getall(self):
        return self.attrs

    def exists(self, k):
        return k in self.attrs

    def delete(self):
        """ Delete the object from redis """
        self.attrs = {}
        return self.redis_client.delete(self._redis_ns)


class PollValue(object):
    class SubscriptionClosedException(RuntimeError):
        pass

    class SubscriberDoesNotExist(IndexError):
        pass

    def __init__(self, poll_name):
        self.redis_client = redis.StrictRedis()

        self.poll_name = '{0}:PollValue'.format(poll_name)
        self.scripts = {}

    def _get_script(self, name, lua):
        if name not in self.scripts:
            self.scripts[name] = self.redis_client.register_script(lua)
        return self.scripts[name]

    def subscribe(self, name):
        lua = """
        local poll_name = KEYS[1]
        local subscriber_name = ARGV[1]

        -- return with no errors if already subscribed
        if redis.call('HEXISTS', poll_name, subscriber_name) ~= 0 then
            return 2 -- already subscribed
        end

        -- return error if subscriptions are closed (votes started)
        for i, v in ipairs(redis.call('HVALS', poll_name)) do
            local parsed = cmsgpack.unpack(v)

            if parsed ~= nil then
                return 0 -- subscription closed
            end
        end

        redis.call('HSET', poll_name, subscriber_name, cmsgpack.pack(nil))
        return 1 -- subscribe
        """

        script = self._get_script('subscribe', lua)
        res = script(keys=[self.poll_name], args=[name])
        if res == 0:
            raise self.SubscriptionClosedException(
                'Subscriptions are not possible after the voting phase started'
            )
        return res

    def vote(self, subscriber_name, vote):
        lua = """
        local poll_name = KEYS[1]
        local subscriber_name = ARGV[1]
        local value = ARGV[2]

        if not redis.call('HEXISTS', poll_name, subscriber_name) then
            return 0 -- voter didn't subscribe or nulled the vote
        end

        local val = redis.call('HGET', poll_name, subscriber_name)

        local parsed_val = cmsgpack.unpack(val)
        if parsed_val ~= nil then
            return 1 -- already voted
        end

        redis.call('HSET', poll_name, subscriber_name, value)

        local everyone_voted = true
        for i, v in ipairs(redis.call('HVALS', poll_name)) do
            local parsed = cmsgpack.unpack(v)
            if parsed == nil then
                everyone_voted = false
            end
        end

        if everyone_voted then
            return 3 -- poll complete
        end

        return 2 -- voted
        """

        script = self._get_script('vote', lua)
        res = script(keys=[self.poll_name],
                     args=[subscriber_name, msgpack.dumps(vote)])
        if res == 0:
            raise self.SubscriberDoesNotExist(
                '{0} did\'nt subscribe and therefore is not allowed to vote'
                .format(subscriber_name)
            )
        return res

    def null_vote(self, subscriber_name):
        lua = """
        local poll_name = KEYS[1]
        local subscriber_name = ARGV[1]

        local val = redis.call('HGET', poll_name, subscriber_name)
        if not val then
            return 0 -- voter didn't subscribe
        end

        val = cmsgpack.unpack(val)
        if val ~= nil then
            return 1 -- already voted
        end

        redis.call('HDEL', poll_name, subscriber_name)

        local everyone_voted = true
        for i, v in ipairs(redis.call('HVALS', poll_name)) do
            local parsed = cmsgpack.unpack(v)
            if parsed == nil then
                everyone_voted = false
            end
        end

        if everyone_voted then
            return 3 -- poll complete
        end

        return 2 -- voted
        """

        script = self._get_script('null_vote', lua)
        res = script(keys=[self.poll_name], args=[subscriber_name])
        return res

    @property
    def votes(self):
        if self.redis_client.hlen(self.poll_name):
            return {
                k: msgpack.loads(v)
                for k, v
                in self.redis_client.hgetall(self.poll_name).iteritems()
            }
        return {}

    @property
    def values(self):
        if self.redis_client.hlen(self.poll_name):
            return map(
                msgpack.loads,
                self.redis_client.hgetall(self.poll_name).values()
            )
        return []
