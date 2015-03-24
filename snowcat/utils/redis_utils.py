import redis
import msgpack


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
        self._obj_setattr('namespace', namespace)
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
        p = self.r_client.pipeline()
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
        object.__setattr__(self, 'attrs', default)
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
