from __future__ import absolute_import

import time
import pickle
import redis
import random

from .celery import celeryapp
from .base import Categorizer, singleton_task


class RandomCategorizer(Categorizer):
    """
    Just a stupid example on how a categorizer should work.
    This very serious and useful categorizer just pops a point from the source
    (no need for another buffer, since it elaborates one point at a time) and
    waits for a second.
    """

    @classmethod
    @celeryapp.task
    @singleton_task
    def run(cls, auth_id):
        redis_client = redis.StrictRedis()

        with open('output.txt', 'wb') as f:
            while True:
                raw_data = redis_client.lpop(cls.gen_key(auth_id))
                if raw_data is None:
                    break
                data = pickle.loads(raw_data)

                # DO SOMETHING WITH THE DATA
                f.write('{0} :: {1}\n'.format(data['ts'], data['type']))
