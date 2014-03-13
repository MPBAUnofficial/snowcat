from __future__ import absolute_import
from abc import ABCMeta, abstractmethod
from celery import chain
from celery.utils.log import get_task_logger
from functools import wraps
from .celery import celeryapp

import redis
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


class Categorizer(object):
    __metaclass__ = ABCMeta

    @classmethod
    def gen_key(cls, user_auth_id, key=''):
        return '{0}_{1}{2}'.format(
            cls.__name__,
            user_auth_id,
            '_' + str(key) if key else ''
        )

    @classmethod
    @celeryapp.task
    def add_data(cls, data):
        if type(data) == dict:
            data = [data]

        # add data to buffer
        # assume data is sorted by timestamp
        auth_ids = set()
        for d in data:
            auth_id = d['auth_user_id']
            auth_ids.add(auth_id)
            redis_client.rpush(cls.gen_key(auth_id), pickle.dumps(d))

        # run the categorizer on all the sessions which has been changed.
        # if the categorizer is already running on the session, do nothing
        for auth_id in auth_ids:
            cls.run.delay(cls, auth_id)

    @classmethod
    @singleton_task
    @abstractmethod
    def run(cls, auth_id):
        """
        Main task.
        Should NOT be called directly, call '<Categorizer>.add_data' instead.
        """
        pass

    @classmethod
    @celeryapp.task
    def close_session(cls, auth_user_id):
        for item in redis_client.keys('{0}*'.format(cls.gen_key(auth_user_id))):
            redis_client.delete(item)

