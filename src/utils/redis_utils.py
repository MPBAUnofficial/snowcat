import redis
import pickle


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

    use_pickle = kwargs.pop('pickle', True)

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

        script = self.redis_client.register_script(lua)
        return script(keys=[key], args=values)

    def llen(self, key):
        res = self.redis_client.hget(key, '__length__')
        if res is None:
            return 0
        return res

    def lindex(self, key, index):
        lua = """
        local key = KEYS[1]

        -- get list length
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

        -- actual index (taking offset into consideration)
        local idx = tonumber(ARGV[1])
        if idx < 0 then idx = idx + length end
        idx = idx + offset

        return redis.call('HGET', key, idx)
        """

        script = self.redis_client.register_script(lua)
        return script(keys=[key], args=[index])

    def lrange(self, key, start, stop):
        lua = """
        local key = KEYS[1]

        local offset = tonumber(redis.call('HGET', key, '__offset__'))
        if not offset then
            offset = 0
            redis.call('HSET', key, '__offset__', 0)
        end

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

        script = self.redis_client.register_script(lua)
        return script(keys=[key], args=[start, stop])

    def get_offset(self, key):
        return self.redis_client.hget(key, '__offset__')

    def delete(self, key):
        return self.redis_client.delete(key)

    def remfirstn(self, key, n):
        """
        Remove first n elements from list
        """
        lua = """
        local key = KEYS[1]
        local offset = tonumber(redis.call('HGET', key, '__offset__'))
        if not offset then
            offset = 0
            redis.call('HSET', key, '__offset__', 0)
        end

        -- check n
        local list_length = tonumber(redis.call('HGET', key, '__length__'))
        local n = tonumber(ARGV[1])
        if n > list_length then
            n = list_length
        end

        -- adjust offset
        redis.call('HSET', key, '__offset__', offset + n)

        -- adjust length
        local list_length = tonumber(redis.call('HGET', key, '__length__'))
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
        script = self.redis_client.register_script(lua)
        return script(keys=[key], args=[n])
