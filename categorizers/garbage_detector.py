from __future__ import absolute_import

import pickle
import redis

from .celeryapp import celeryapp
from .base import Categorizer, singleton_task
from .utils import redis_mget, redis_mset, redis_lindex


# todo: replace those helper functions with real res backend
# (this is a temporary solution, since result backend is to be defined yet)
def set_garbage(point):
    r = redis.StrictRedis()
    return r.sadd('garbage_points', point)


def is_garbage(point):
    r = redis.StrictRedis()
    return r.sismember('garbage_points', point)


class GarbagePointCategorizer(Categorizer):
    """
    Detect garbage points (points outside the tracks, or points which don't
    seem to be believable).
    """
    ID = 'GarbagePointCategorizer'
    BACK_OFFSET = 0
    DEPENDENCIES = []

    PRECISION_THRESHOLD = 50  # m

    @classmethod
    @celeryapp.task
    @singleton_task
    def run(cls, auth_id):
        idx = redis_mget(cls.gen_key(auth_id), ('idx', 0))

        while True:
            raw_data = redis_lindex(cls.gen_key(auth_id), idx)

            if (idx % 100) == 0 or raw_data is None:
                redis_mset(cls.gen_key(auth_id), idx=idx)
                cls.call_children(auth_id)

                if raw_data is None:
                    break

            p = pickle.loads(raw_data)
            if p['m'] >= cls.PRECISION_THRESHOLD:
                set_garbage(p['id'])
            if 0 in (p['x'], p['y'], p['z']):
                set_garbage(p['id'])

