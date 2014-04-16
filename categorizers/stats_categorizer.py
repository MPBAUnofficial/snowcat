from __future__ import absolute_import

import redis
import pickle
import psycopg2
import psycopg2.extras
from celery.utils.log import get_task_logger

from .base import Categorizer, singleton_task, celeryapp
from .utils import distance_3d

from .local_db_settings import DB_SETTINGS

logger = get_task_logger(__name__)


class StatsCategorizer(Categorizer):
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
            try:
                prev_item = buf[i-1]
                item = buf[i]
                suc_item = buf[i+1]
            except IndexError:
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

            out_dict = {'speed' : str(speed)}

            with psycopg2.connect(**DB_SETTINGS) as conn:
                psycopg2.extras.register_hstore(conn)
                with conn.cursor() as cur:
                    cur.execute("UPDATE skilo_sc.user_location_track \
                                 SET categorizers = categorizers || %s \
                                 WHERE id=%s",[out_dict,item['id']])
