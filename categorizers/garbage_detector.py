from __future__ import absolute_import

import pickle

from .celery import celeryapp
from .base import Categorizer, redis_client


class GarbagePointCategorizer(Categorizer):
    """
    Detect garbage points (points outside the tracks, or points which don't
    seem to be believable.
    """

    BUFFER_LENGTH = 10

    @classmethod
    @celeryapp.task
    def _run(cls, auth_id):
        while True:
            # pop data
            raw_data = redis_client.lpop(cls.gen_key(auth_id))
            if raw_data is None:
                break

            # insert data in buffer
            buf_key = cls.gen_key(auth_id, 'buf')
            redis_client.rpush(buf_key, raw_data)
            if redis_client.llen(buf_key) >= cls.BUFFER_LENGTH:
                redis_client.lpop(buf_key)

            buffer = [
                pickle.loads(item)
                for item in redis_client.lrange(buf_key, 0, -1)
            ]

            # ???
            # profit.