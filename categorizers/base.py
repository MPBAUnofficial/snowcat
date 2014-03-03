from __future__ import absolute_import
from abc import ABCMeta, abstractmethod
from celery import chain
from .celery import celeryapp

import redis
import pickle

redis_client = redis.StrictRedis()
LOCK_EXPIRE = 60 * 10


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
            lock_key = cls.gen_key(auth_id, 'lock')
            # todo: is a lock TTL _really_ necessary?
            lock = redis_client.lock(lock_key, timeout=LOCK_EXPIRE)

            have_lock = lock.acquire(blocking=False)
            if have_lock:
                cls._run.apply_async(
                    (cls, auth_id),
                    link=cls.unlock.si(lock_key)
                )

    @staticmethod
    @celeryapp.task
    def unlock(lock_key):
        print '======= UNLOCKING ========'
        lock = redis_client.lock(lock_key)
        try:
            lock.release()
        except ValueError:
            redis_client.delete(lock_key)

    @classmethod
    @abstractmethod
    def _run(cls, auth_id):
        """
        Main task.
        Must NOT be called directly, call '<Categorizer>.add_data(...)' instead.
        """
        pass

    @classmethod
    @celeryapp.task
    def close_session(cls, auth_user_id):
        for item in redis_client.keys('{0}*'.format(cls.gen_key(auth_user_id))):
            redis_client.delete(item)

