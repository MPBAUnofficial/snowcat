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

    @property
    def _redis_key(self):
        """ Generate the redis key for this PersistentObject """
        return '{0}:PersistentObject'.format(self.key)

    def save(self):
        """ Save the data on redis """
        r = object.__getattribute__(self, 'redis_client')
        r.set(self._redis_key, msgpack.dumps(self.attrs))

    def load(self):
        """ Load the data from redis"""
        r = object.__getattribute__(self, 'redis_client')
        serialized = r.get(self._redis_key)
        if serialized is not None:
            stored_val = msgpack.loads(serialized)
            self.attrs.update(stored_val or {})

    def get(self, attr, default=None):
        """ PO.get(k[,d]) -> D[k] if k in PO, else d.  d defaults to None. """
        attrs = object.__getattribute__(self, 'attrs')
        if attr in attrs:
            return attrs[attr]
        return default

    def delete(self):
        """ Delete the PersistentObject from redis. """
        r = object.__getattribute__(self, 'redis_client')
        r.delete(self._redis_key)
