import utm
import numpy
import redis
from redis.exceptions import WatchError


def distance_2d(lon1, lat1, lon2, lat2):
    ym1, xm1, _, _ = utm.from_latlon(lat1, lon1)
    ym2, xm2, _, _ = utm.from_latlon(lat2, lon2)

    arr1 = numpy.array((xm1, ym1))
    arr2 = numpy.array((xm2, ym2))

    dist = numpy.linalg.norm(arr1-arr2)
    return dist


def distance_3d(lon1, lat1, h1, lon2, lat2, h2):
    ym1, xm1, _, _ = utm.from_latlon(lat1, lon1)
    ym2, xm2, _, _ = utm.from_latlon(lat2, lon2)

    arr1 = numpy.array((xm1, ym1, h1))
    arr2 = numpy.array((xm2, ym2, h2))

    dist = numpy.linalg.norm(arr1-arr2)
    return dist


# == REDIS UTILS ==

def redis_mget(namespace, *args, **kwargs):
    """
    Returns the values of all specified keys. For every key that does not exist,
    the default value (if any) is returned.
    Keys can be specified as strings or as tuples where the first element is the
    actual key and the second element is the default value for that key.
    """
    redis_client = kwargs.get('redis_client', redis.StrictRedis())
    pipe = redis_client.pipeline()

    defaults = []
    for arg in args:
        a = None
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

    return [
        val if val is not None else d
        for val, d in zip(res, defaults)
    ]


def redis_mset(namespace, redis_client=None, **kwargs):
    if redis_client is None:
        redis_client = redis.StrictRedis()

    for k, v in kwargs.iteritems():
        redis_client.set('{0}_cache_{1}'.format(namespace, k), v)


class RedisList(object):
    """
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
        -- adjust list length
        local length = tonumber(redis.call('GET', KEYS[1] .. '_length'))
        if not length then
            length = 0
            redis.call('SET', KEYS[1] .. '_length', length)
        end
        length = length + #ARGV
        redis.call('SET', KEYS[1] .. '_length', length)

        -- get offset
        local offset = tonumber(redis.call('GET', KEYS[1] .. '_offset'))
        if not offset then
            offset = 0
            redis.call('SET', KEYS[1] .. '_offset', offset)
        end

        -- insert values
        for i, v in pairs(ARGV) do
            redis.call('HSET', KEYS[1], offset+i-1, v)
        end
        return length
        """

        script = self.redis_client.register_script(lua)
        return script(keys=[key], args=values)

    def llen(self, key):
        res = self.redis_client.get('{0}_length'.format(key))
        if res is None:
            return 0
        return res

    def lindex(self, key, index):
        lua = """
        -- get offset
        local offset = tonumber(redis.call('GET', KEYS[1] .. '_offset'))
        if not offset then
            offset = 0
            redis.call('SET', KEYS[1] .. '_offset', offset)
        end

        -- actual index (taking offset into consideration)
        local idx = offset + ARGV[1]
        return redis.call('HGET', KEYS[1], idx)
        """

        script = self.redis_client.register_script(lua)
        return script(keys=[key], args=[index])

    def lrange(self, key, start, stop):
        lua = """
        local offset = tonumber(redis.call('GET', KEYS[1] .. '_offset'))
        if not offset then
            offset = 0
            redis.call('SET', KEYS[1] .. '_offset', 0)
        end

        local start = tonumber(ARGV[1])
        local stop = tonumber(ARGV[2])
        local list_length = tonumber(redis.call('GET', KEYS[1] .. '_length'))

        -- adjust start and stop values
        if start < 0 then start = list_length + start -1 end
        if stop < 0 then stop = list_length + stop -1 end
        start = start + offset
        stop = stop + offset

        -- get the values and return them
        local res = {}
        local idx = 1
        for i = start, stop do
            res[idx] = redis.call('HGET', KEYS[1], i)
            idx = idx + 1
        end
        return res
        """

        script = self.redis_client.register_script(lua)
        return script(keys=[key], args=[start, stop])

    def get_offset(self, key):
        return self.redis_client.get('{0}_offset'.format(key))

    def delete(self, key):
        return self.redis_client.delete(key, '{0}_offset'.format(key), '{0}_length'.format(key))

    def remfirstn(self, key, n):
        """
        Remove first n elements from list
        """
        lua = """
        local offset = tonumber(redis.call('GET', KEYS[1] .. '_offset'))
        if not offset then
            offset = 0
            redis.call('SET', KEYS[1] .. '_offset', 0)
        end

        -- check n
        local list_length = tonumber(redis.call('GET', KEYS[1] .. '_length'))
        local n = tonumber(ARGV[1])
        if n > list_length then
            n = list_length
        end

        -- adjust offset
        redis.call('SET', KEYS[1] .. '_offset', offset + n)

        -- adjust length
        local list_length = tonumber(redis.call('GET', KEYS[1] .. '_length'))
        if not list_length then return 0 end
        redis.call('SET', KEYS[1] .. '_length', list_length - n)

        -- remove first n elements
        local args_list = {}
        for i = 0, n do
            args_list[i] = i + offset
        end

        redis.call('HDEL', KEYS[1], args_list)
        return n
        """
        script = self.redis_client.register_script(lua)
        return script(keys=[key], args=[n])
