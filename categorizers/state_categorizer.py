from __future__ import absolute_import

import redis
import pickle
from celery.utils.log import get_task_logger

from .base import Categorizer, singleton_task, celeryapp
from .utils import distance_3d, distance_2d, redis_mget, redis_mset, RedisList

logger = get_task_logger(__name__)


class StateCategorizer(Categorizer):
    ID = 'GarbagePointCategorizer'
    BACK_OFFSET = 1
    DEPENDENCIES = []

    @classmethod
    @celeryapp.task
    @singleton_task
    def run(cls, auth_id):
        rl = RedisList()

        idx = redis_mget(cls.gen_key(auth_id), ('idx', 0))
        prev_item = None

        while True:
            raw_data = rl.lindex(cls.gen_key(auth_id), idx)

            if raw_data is None:
                break

            if (idx % 100) == 0 or raw_data is None:  # periodic check
                redis_mset(cls.gen_key(auth_id), idx=max(0, idx-1))
                cls.call_children(auth_id)

            item = pickle.loads(raw_data)

            if prev_item is None:
                prev_item = item
                continue

            distance2d = distance_2d(
                item['x'], item['y'],
                prev_item['x'], prev_item['y']
            )

            distance3d = distance_3d(
                item['x'], item['y'], item['z'],
                prev_item['x'], prev_item['y'], prev_item['z']
            )

            time_delta = abs(item['ts'] - prev_item['ts'])

            speed2d = (distance2d / time_delta) * 3.6
            speed3d = (distance3d / time_delta) * 3.6

            error_delta = item['m'] - prev_item['m']

            classifications = \
                dict(
                    distance2d=distance2d, distance3d=distance3d,
                    time_delta=time_delta, speed2d=speed2d,
                    speed3d=speed3d, error_delta=error_delta
                )

            # todo: salvare classificazioni da qualche parte

            prev_item = item
