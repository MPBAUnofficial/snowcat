from __future__ import absolute_import

import redis
import pickle
import psycopg2
import psycopg2.extras
from celery.utils.log import get_task_logger

from .base import Categorizer, singleton_task, celeryapp
from .utils import distance_3d, redis_mget, redis_mset, RedisList

from .local_settings import DATABASES

logger = get_task_logger(__name__)


class StatsCategorizer(Categorizer):
    ID = 'StatsCategorizer'
    BACK_OFFSET = 2
    DEPENDENCIES = []

    BUFFER_TIMEDELTA = 60 * 5  # s
    BUFFER_MIN_SIZE = 10

    @classmethod
    @celeryapp.task
    @singleton_task
    def run(cls, auth_id):
        rl = RedisList()

        idx = redis_mget(cls.gen_key(auth_id), ('idx', 0))
        prev_item = None
        item = None

        while True:
            raw_data = rl.lindex(cls.gen_key(auth_id), idx)

            if raw_data is None:
                break

            if (idx % 100) == 0 or raw_data is None:
                redis_mset(cls.gen_key(auth_id), idx=max(0, idx-2))
                cls.call_children(auth_id)

            suc_item = pickle.loads(raw_data)

            if item is None:
                item = suc_item
                continue
            if prev_item is None:
                prev_item = item
                item = suc_item
                continue

            #compute speed: previous and successive points are considered
            distance_prev = distance_3d(
                item['x'], item['y'], item['z'],
                prev_item['x'], prev_item['y'], prev_item['z']
            )
            distance_suc = distance_3d(
                item['x'], item['y'], item['z'],
                suc_item['x'], suc_item['y'], suc_item['z']
            )

            time_delta = abs(suc_item['ts'] - prev_item['ts'])

            speed = (distance_prev+distance_suc) / time_delta

            out_dict = {'speed': str(speed)}
            redis_mset(cls.gen_key(auth_id), out=out_dict)


            # with psycopg2.connect(**DB_SETTINGS) as conn:
            #     psycopg2.extras.register_hstore(conn)
            #     with conn.cursor() as cur:
            #         cur.execute("UPDATE skilo_sc.user_location_track \
            #                      SET categorizers = categorizers || %s \
            #                      WHERE id=%s",[out_dict,item['id']])
