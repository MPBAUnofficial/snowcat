import redis
import msgpack


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
        r.set('{0}:PersistentObject'.format(self.key), msgpack.dumps(self.attrs))

    def load(self):
        r = object.__getattribute__(self, 'redis_client')
        serialized = r.get('PersistentObject:{0}'.format(self.key))
        if serialized is not None:
            stored_val = msgpack.loads(serialized)
            self.attrs.update(stored_val or {})

    def get(self, attr, default=None):
        attrs = object.__getattribute__(self, 'attrs')
        if attr in attrs:
            return attrs[attr]
        return default








