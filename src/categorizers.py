from abc import ABCMeta, abstractmethod
from celery.utils.log import get_task_logger
from functools import wraps
from utils.redis_utils import RedisList, redis_mget, redis_mset
from utils.geo import distance_2d, distance_3d

import redis
from celeryapp import celeryapp
import pickle


logger = get_task_logger(__name__)

redis_client = redis.StrictRedis()
LOCK_EXPIRE = 60 * 10


def singleton_task(func):
    """
    Decorator to make the task a pseudo-singleton.
    Enables a maximum of one task to be executed for each session
    for each categorizer (i.e. there can't be more than one RandomCategorizer
    running on session with auth_user_id 42).
    """
    @wraps(func)
    def _inner(cls, auth_id, *args, **kwargs):
        # try to acquire lock
        lock_key = cls.gen_key(auth_id, 'lock')
        logger.info('TRYING TO ACQUIRE LOCK {0}'.format(lock_key))

        lock = redis_client.lock(lock_key, timeout=LOCK_EXPIRE)
        have_lock = lock.acquire(blocking=False)
        logger.info('Acquired {0}? {1}'.format(lock_key, have_lock))

        if have_lock:
            try:
                func(cls, auth_id, *args, **kwargs)
            finally:
                logger.info('RELEASING LOCK {0}'.format(lock_key))
                lock.release()
    return _inner


@celeryapp.task
def add_data(data):
    r = redis.StrictRedis()
    rl = RedisList(redis_client=r)

    if type(data) == dict:
        data = [data]

    auth_ids = set()
    for d in data:
        # todo: split data when more tracks are provided in the same request
        auth_id = d['auth_user_id']
        auth_ids.add(auth_id)
        rl.rpush('Points_{0}'.format(auth_id), pickle.dumps(d))

    root_categorizers = r.smembers('root_categorizers')

    for cat in all_categorizers:
        if cat.ID in root_categorizers:
            logger.warning("launching categorizer {0}".format(cat.ID))
            for auth_id in auth_ids:
                cat.run.delay(cat, auth_id)


def get_categorizer(cat_id):
    """
    This is ugly, but since we won't have thousands of categorizers,
    it will be efficient enought.
    """
    for cat in all_categorizers:
        if cat.ID == cat_id:
            return cat
    return None


class Categorizer(object):
    __metaclass__ = ABCMeta

    ID = ''
    DEPENDENCIES = []
    BACK_OFFSET = 0  # negative values mean 'infinite'
    # DEPSCHECK_FREQUENCY = 1000  # check for dependencies every N operations
    # GC_FREQUENCY = 0  # same as above, 0 = never call gc

    @classmethod
    def gen_key(cls, user_auth_id, key=''):
        return '{0}_{1}{2}'.format(
            cls.ID or cls.__name__,
            user_auth_id,
            '_' + str(key) if key else ''
        )

    @classmethod
    def children(cls):
        return redis_client.smembers('{0}_children'.format(cls.ID))

    @classmethod
    def call_children(cls, auth_id):
        children = cls.children()

        for cat in all_categorizers:
            if cat.ID in children:
                cat.run.delay(auth_id)

    @classmethod
    @singleton_task
    @abstractmethod
    def run(cls, auth_id):
        """ Main task.

        Should NOT be called directly, call '<Categorizer>.add_data' instead.
        """
        pass

    @classmethod
    @celeryapp.task
    def close_session(cls, auth_user_id):
        for item in redis_client.keys('{0}*'.format(cls.gen_key(auth_user_id))):
            redis_client.delete(item)


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
        idx = int(idx)
        prev_item = None
        item = None

        while True:
            raw_data = rl.lindex(cls.gen_key(auth_id), idx)

            if raw_data is None:
                redis_mset(cls.gen_key(auth_id), idx=max(0, idx-2))
                break

            if (idx % 100) == 0 or raw_data is None:
                redis_mset(cls.gen_key(auth_id), idx=max(0, idx-2))
                cls.call_children(auth_id)

            next_item = pickle.loads(raw_data)

            if item is None:
                item = next_item
                continue
            if prev_item is None:
                prev_item = item
                item = next_item
                continue

            #compute speed: previous and successive points are considered
            distance_prev = distance_3d(
                item['x'], item['y'], item['z'],
                prev_item['x'], prev_item['y'], prev_item['z']
            )
            distance_suc = distance_3d(
                item['x'], item['y'], item['z'],
                next_item['x'], next_item['y'], next_item['z']
            )

            time_delta = abs(next_item['ts'] - prev_item['ts'])

            speed = (distance_prev+distance_suc) / time_delta

            out_dict = {'speed': str(speed)}
            redis_mset(cls.gen_key(auth_id), out=out_dict)


            # with psycopg2.connect(**DB_SETTINGS) as conn:
            #     psycopg2.extras.register_hstore(conn)
            #     with conn.cursor() as cur:
            #         cur.execute("UPDATE skilo_sc.user_location_track \
            #                      SET categorizers = categorizers || %s \
            #                      WHERE id=%s",[out_dict,item['id']])


class StateCategorizer(Categorizer):
    ID = 'StateCategorizer'
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


all_categorizers = [
    #    GarbagePointCategorizer,
    #    StateCategorizer
    StatsCategorizer
]
