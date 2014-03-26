from __future__ import absolute_import

import redis
import pickle
from celery.utils.log import get_task_logger

from .base import Categorizer, singleton_task, celeryapp
from .utils import m2_distance, m3_distance

logger = get_task_logger(__name__)


class StateCategorizer(Categorizer):
    BUFFER_TIMEDELTA = 60 * 5  # s
    BUFFER_MIN_SIZE = 10

    @classmethod
    @celeryapp.task
    @singleton_task
    def run(cls, auth_id):
        redis_client = redis.StrictRedis()

        buf_key = cls.gen_key(auth_id)
        buf = [
            pickle.loads(item) for item in redis_client.lrange(buf_key, 0, -1)
        ]

        last_idx = int(redis_client.get(cls.gen_key(auth_id, 'curr_idx')) or 0)
        # STEP 1
        for i in range(last_idx, len(buf)):
            if i == 0:
                continue
            item = buf[i]
            prev_item = buf[i-1]

            distance2d = m2_distance(
                item['x'], item['y'],
                prev_item['x'], prev_item['y']
            )

            distance3d = m3_distance(
                item['x'], item['y'], item['z'],
                prev_item['x'], prev_item['y'], prev_item['z']
            )

            time_delta = abs(item['ts'] - prev_item['ts'])

            speed2d = (distance2d / time_delta) * 3.6
            speed3d = (distance3d / time_delta) * 3.6

            error_delta = item['m'] - prev_item['m']

            item['classifications'] = \
                dict(
                    distance2d=distance2d, distance3d=distance3d,
                    time_delta=time_delta, speed2d=speed2d,
                    speed3d=speed3d, error_delta=error_delta
                )

        # STEP 2
        # todo: salvare il buffer in redis

        current_index_key = cls.gen_key(auth_id, 'curr_idx')
        redis_client.set(current_index_key, len(buf) - 1)


        # while True:
        #     # pop data
        #     raw_data = redis_client.lpop(cls.gen_key(auth_id))
        #     if raw_data is None:
        #         break
        #
        #     buf_key = cls.gen_key(auth_id, 'buf')
        #
        #     # put data in local buffer
        #     redis_client.rpush(buf_key, raw_data)
        #
        #     if redis_client.llen(buf_key) < 3:
        #         # we need at least n (3?) values in the buffer
        #         continue
        #
        #     # get the idx-th element's timestamp from buffer in redis
        #     buf_val_ts = lambda idx: \
        #         pickle.loads(redis_client.lrange(buf_key, idx, idx)[0])['ts']
        #
        #     # clean the buffer
        #     while abs(buf_val_ts(-1) - buf_val_ts(1)) >= cls.BUFFER_TIMEDELTA \
        #             and not redis_client.llen(buf_key) <= cls.BUFFER_MIN_SIZE:
        #         redis_client.lpop(buf_key)
        #
        #     buf = [
        #         pickle.loads(item)
        #         for item in redis_client.lrange(buf_key, 0, -1)
        #         # todo: exclude garbage points?
        #     ]
        #
        #     logger.info('size: {}, ts_delta: {}'
        #                 .format(len(buf), abs(buf[0]['ts'] - buf[-1]['ts'])))
        #
        #     # -- END OF BUFFER SMANDRUPPATION --


