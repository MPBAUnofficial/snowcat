from __future__ import absolute_import

import pickle
import time

from .celeryapp import celeryapp
from .base import Categorizer, singleton_task
from .utils import redis_mget, redis_mset, RedisList


class RandomCategorizer(Categorizer):
    """
    Just a stupid example on how a categorizer should work.
    This very serious and useful categorizer just pops a point from the source
    and waits for a second.
    """
    ID = 'RandomCategorizer'
    BACK_OFFSET = 0
    DEPENDENCIES = []

    @classmethod
    @celeryapp.task
    @singleton_task
    def run(cls, auth_id):
        rl = RedisList()

        idx = redis_mget(cls.gen_key(auth_id), ('idx', 0))

        with open('output.txt', 'wb') as f:
            while True:
                raw_data = rl.lindex(cls.gen_key(auth_id), idx)

                if raw_data is None:
                    break

                if (idx % 100) == 0 or raw_data is None:  # periodic check
                    redis_mset(cls.gen_key(auth_id), idx=idx)  # save status on redis
                    cls.call_children(auth_id)  # wake up (if they're not already running) children catalogators

                data = pickle.loads(raw_data)

                # DO SOMETHING WITH THE DATA
                # f.write('{0} :: {1}\n'.format(data['ts'], data['type']))
                time.sleep(1)

                idx += 1
