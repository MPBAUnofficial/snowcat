from __future__ import absolute_import

import pickle

from .celery import celeryapp
from .base import Categorizer, redis_client, singleton_task


# todo: replace those helper functions with real res backend
# (this is a temporary solution, since result backend is to be defined yet)
def set_garbage(point):
    return redis_client.sadd('garbage_points', point)


def is_garbage(point):
    return redis_client.sismember('garbage_points', point)


class GarbagePointCategorizer(Categorizer):
    """
    Detect garbage points (points outside the tracks, or points which don't
    seem to be believable).
    """
    PRECISION_THRESHOLD = 50  # m

    @classmethod
    @celeryapp.task
    @singleton_task
    def run(cls, auth_id):
        while True:
            # pop data
            raw_data = redis_client.lpop(cls.gen_key(auth_id))
            if raw_data is None:
                break

            p = pickle.loads(raw_data)
            if p['m'] >= cls.PRECISION_THRESHOLD:
                set_garbage(p['id'])
            if 0 in (p['x'], p['y'], p['z']):
                set_garbage(p['id'])

