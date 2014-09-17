import redis
import pickle
import time


class PersistentObject(object):
    """
    An easy way to save and restore a bunch of variables wrapping themselves
    into an object and saving/loading the object on/from redis.
    """

    def __init__(self, key, default=None, save_on_write=False):
        if default is None:
            default = {}
        self._obj_setattr('key', key)
        self._obj_setattr('attrs', default)
        self._obj_setattr('save_on_write', save_on_write)
        self._obj_setattr('redis_client', redis.StrictRedis())
        self.load()

    def __getattribute__(self, item):
        attrs = object.__getattribute__(self, 'attrs')
        if item in attrs:
            return attrs[item]
        return object.__getattribute__(self, item)

    def __getattr__(self, item):
        return object.__getattribute__(self, item)

    def __setattr__(self, key, value):
        attrs = object.__getattribute__(self, 'attrs')
        attrs[key] = value

        if self.save_on_write:
            self.save()

    def __repr__(self):
        return repr(object.__getattribute__(self, 'attrs'))

    def __str__(self):
        return str(object.__getattribute__(self, 'attrs'))

    def _obj_setattr(self, name, value):
        object.__setattr__(self, name, value)

    def _obj_getattr(self, name):
        object.__getattribute__(self, name)

    def save(self):
        r = object.__getattribute__(self, 'redis_client')
        r.set('PersistentObject:{0}'.format(self.key), pickle.dumps(self.attrs))

    def load(self):
        r = object.__getattribute__(self, 'redis_client')
        serialized = r.get('PersistentObject:{0}'.format(self.key))
        if serialized is not None:
            stored_val = pickle.loads(serialized)
            self.attrs.update(stored_val or {})


def redis_mget(namespace, *args, **kwargs):
    """
    Returns the values of all specified keys. For every key that does not exist,
    the default value (if any) is returned.
    Keys can be specified as strings or as tuples where the first element is the
    actual key and the second element is the default value for that key.

    Set the 'use_pickle' argument to False to get the raw data, without
    deserializing it with pickle.
    """
    redis_client = kwargs.get('redis_client', redis.StrictRedis())
    pipe = redis_client.pipeline()

    use_pickle = kwargs.pop('use_pickle', True)

    defaults = []

    for arg in args:
        d = kwargs.get('global_default_val', None)

        if isinstance(arg, (list, tuple)):
            if len(arg) > 2:
                raise ValueError("You must provide either a string or a tuple.")
            a = arg[0]
            d = arg[1] if len(arg) == 2 else None
        else:
            a = arg

        pipe.get('{0}_cache_{1}'.format(namespace, a))
        defaults.append(d)

    res = pipe.execute()

    get_val = lambda k: val if not use_pickle else pickle.loads(val)

    res = [
        get_val(val) if val is not None else d
        for val, d in zip(res, defaults)
    ]
    return res[0] if len(res) == 1 else res


def redis_mset(namespace, redis_client=None, use_pickle=True, **kwargs):
    """ Set one or more key-value pair to redis.

    Set the 'use_pickle' argument to False in order to put raw data in redis
    instead of pickled data.
    """
    if redis_client is None:
        redis_client = redis.StrictRedis()

    for k, v in kwargs.iteritems():
        if use_pickle:
            v = pickle.dumps(v)
        redis_client.set('{0}_cache_{1}'.format(namespace, k), v)


# TODO: register the script only the first time it is executed, then reuse it.
class RedisList(object):
    """ An alternative list implementation for redis based on HashSets.

    The only implementation of lists available in redis is implemented as a linked-list.
    Accessing an element by index in a linked-list costs O(N).
    This class provides an alternative implementation based on hashsets.
    It's ugly, but it's way more efficient than native lists in accessing values by index.
    The usage is quite the same of redis lists
    """
    def __init__(self, redis_client=None, redis_db=0):
        self.redis_client = redis_client or redis.StrictRedis(db=redis_db)
        self.scripts = {}

    def rpush(self, key, *values):
        lua = """
        local key = KEYS[1]

        -- adjust list length
        local length = tonumber(redis.call('HGET', key, '__length__'))

        if not length then
            length = 0
            redis.call('HSET', key, '__length__', length)
        end

        -- get offset
        local offset = tonumber(redis.call('HGET', key, '__offset__'))
        if not offset then
            offset = 0
            redis.call('HSET', key, '__offset__', offset)
        end

        -- insert values
        for i, v in ipairs(ARGV) do
            redis.call('HSET', key, offset+length+i-1, v)
        end

        length = length + #ARGV
        redis.call('HSET', key, '__length__', length)

        return length
        """

        if not 'rpush' in self.scripts:
            self.scripts['rpush'] = self.redis_client.register_script(lua)
        script = self.scripts['rpush']
        return script(keys=[key], args=values)

    def llen(self, key):
        res = self.redis_client.hget(key, '__length__')
        if res is None:
            return 0
        return int(res)

    def lindex(self, key, index):
        lua = """
        local key = KEYS[1]

        -- get list length
        local length = tonumber(redis.call('HGET', key, '__length__'))

        -- get offset
        local offset = tonumber(redis.call('HGET', key, '__offset__'))

        -- actual index (taking offset into consideration)
        local idx = tonumber(ARGV[1])
        if idx < 0 then idx = idx + length end
        idx = idx + offset

        return redis.call('HGET', key, idx)
        """

        if not 'lindex' in self.scripts:
            self.scripts['lindex'] = self.redis_client.register_script(lua)
        script = self.scripts['lindex']
        return script(keys=[key], args=[index])

    def mlindex(self, key, *indexes):
        # todo: write test cases for mlindex
        lua = """
        local key = KEYS[1]

        local res = {}
        for i, v in pairs(ARGV) do
            res[i] = redis.call('HGET', key, v)
        end
        return res
        """
        if not 'mlindex' in self.scripts:
            self.scripts['mlindex'] = self.redis_client.register_script(lua)
        script = self.scripts['mlindex']
        return script(keys=[key], args=indexes)

    def lrange(self, key, start, stop):
        lua = """
        local key = KEYS[1]

        local offset = tonumber(redis.call('HGET', key, '__offset__'))

        local start = tonumber(ARGV[1])
        local stop = tonumber(ARGV[2])
        local list_length = tonumber(redis.call('HGET', key, '__length__'))

        -- adjust start and stop values
        if start < 0 then start = list_length + start end
        if stop < 0 then stop = list_length + stop end

        if stop > list_length-1 then stop = list_length-1 end
        if start < 0 then start = 0 end

        start = start + offset
        stop = stop + offset

        -- get the values and return them
        local res = {}
        local idx = 1
        for i = start, stop do
            res[idx] = redis.call('HGET', key, i)
            idx = idx + 1
        end
        return res
        """

        if not 'lrange' in self.scripts:
            self.scripts['lrange'] = self.redis_client.register_script(lua)
        script = self.scripts['lrange']
        return script(keys=[key], args=[start, stop])

    def get_offset(self, key):
        offset = self.redis_client.hget(key, '__offset__')
        if offset is None:
            return 0
        return int(offset)

    def delete(self, key):
        return self.redis_client.delete(key)

    def remfirstn(self, key, n):
        """ Remove first n elements from list
        """
        lua = """
        local key = KEYS[1]
        local offset = tonumber(redis.call('HGET', key, '__offset__'))

        -- check n
        local list_length = tonumber(redis.call('HGET', key, '__length__'))
        local n = tonumber(ARGV[1])
        if n > list_length then
            n = list_length
        elseif n < 0 then
            n = 0
        end

        -- adjust offset
        redis.call('HSET', key, '__offset__', offset + n)

        -- adjust length
        if not list_length then return 0 end
        redis.call('HSET', key, '__length__', list_length - n)

        -- remove first n elements
        local args_list = {}
        for i = 0, n do
            args_list[i] = i + offset
        end

        redis.call('HDEL', key, args_list)
        return n
        """
        if not 'remfirstn' in self.scripts:
            self.scripts['remfirstn'] = self.redis_client.register_script(lua)
        script = self.scripts['remfirstn']
        return script(keys=[key], args=[n])

    def lpop(self, key):
        """ Remove and get the first element in a list
        """
        lua = """
        local key = KEYS[1]
        local offset = tonumber(redis.call('HGET', key, '__offset__'))

        -- adjust offset
        redis.call('HSET', key, '__offset__', offset + 1)

        -- adjust length
        local list_length = tonumber(redis.call('HGET', key, '__length__'))
        if not list_length then return nil end
        redis.call('HSET', key, '__length__', list_length - 1)

        -- remove and return the element
        local res = redis.call('HGET', key, offset)
        redis.call('HDEL', key, offset)
        return res
        """
        if not 'lpop' in self.scripts:
            self.scripts['lpop'] = self.redis_client.register_script(lua)
        script = self.scripts['lpop']
        return script(keys=[key], args=[])

    def killfirstn(self, key, n):
        """ Remove first n elements from a list leaving offset unchanged.
        This way, every element will mantain the same index, and the first n
        elements will just disappear.

        Be careful when using this method: you might mess up everything
        """
        lua = """
        local key = KEYS[1]
        local offset = tonumber(redis.call('HGET', key, '__offset__'))
        --if not offset then
        --    offset = 0
        --    redis.call('HSET', key, '__offset__', 0)
        --end

        -- check n
        local list_length = tonumber(redis.call('HGET', key, '__length__'))
        local n = tonumber(ARGV[1])
        if n > list_length then
            n = list_length
        elseif n < 0 then
            n = 0
        end

        -- adjust offset
        redis.call('HSET', key, '__offset__', offset + n)

        -- adjust length
        if not list_length then return 0 end
        redis.call('HSET', key, '__length__', list_length - n)

        -- remove first n elements
        local args_list = {}
        for i = 0, n do
            args_list[i] = i + offset
        end

        redis.call('HDEL', key, args_list)
        return n
        """
        if not 'killfirstn' in self.scripts:
            self.scripts['killfirstn'] = self.redis_client.register_script(lua)
        script = self.scripts['killfirstn']
        return script(keys=[key], args=[n])


def test_redis_list(redis_client, n):
    r = redis_client
    key = '__benchmark_test_list'
    r.delete(key)
    start = time.time()

    for i in xrange(n):
        r.rpush(key, i)

    for i in xrange(n):
        elem = r.lindex(key, i)
        if elem is not None and (int(elem) % 13) == 0:
            pass
    end = time.time()
    return end - start


def redislist_benchmark(ns):
    r = redis.StrictRedis()
    rl = RedisList()

    for n in ns:
        print 'Testing with {0} values...'.format(n)
        r_bench = test_redis_list(r, n)
        rl_bench = test_redis_list(rl, n)
        print 'Standard: \t{0:.3f} s'.format(r_bench)
        print 'HashSetList: \t{0:.3f} s'.format(rl_bench)
        print

if __name__ == '__main__':
    import sys
    if sys.argv[1] == 'bench':
        if len(sys.argv) == 2:
            ns = (10, 100, 1000, 10000, 100000, 200000, 400000)
        else:
            ns = map(int, sys.argv[2:])
        redislist_benchmark(ns)