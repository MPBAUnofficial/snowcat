from celery import Task
from celery.utils.log import get_task_logger
import redis
from shutil import rmtree
from categorizers import get_root_categorizers, LoopCategorizer
import os


class BaseAddData(Task):
    queue = 'add_data'

    FSQUEUE_PREFIX = '/tmp/snowcat/'

    r = redis.StrictRedis()

    @property
    def logger(self):
        if not hasattr(self, 'logger'):
            self._logger = get_task_logger(self.name)
        return self._logger

    def gen_key(self, user, key=''):
        return '{0}:{1}{2}'.format(
            self.name,
            user,
            ':' + str(key) if key else ''
        )

    def run(self, data, snowcat_queue='Stream', **kwargs):
        root_categorizers = get_root_categorizers(self.app)

        user = data['user']

        LoopCategorizer.save_chunk_fs(
            data['data'] if isinstance(data['data'], (tuple, list))
            else [data['data']],
            os.path.join(self.FSQUEUE_PREFIX, str(user), snowcat_queue, 'queue')
        )

        for cat in root_categorizers:
            cat.run_if_not_already_running(user)

        return True

    def apply_async(self, *args, **kwargs):
        if 'queue' not in kwargs and self.queue:
            kwargs['queue'] = self.queue

        return super(BaseAddData, self).apply_async(*args, **kwargs)

    def delay(self, *args, **kwargs):
        return self.apply_async(args, kwargs)


class FinalizeStream(Task):  # todo: rename class
    """ Task called when all categorizers have finished processing a stream.
    """
    name = 'BaseStreamFinalizer'

    @property
    def logger(self):
        if not hasattr(self, 'logger'):
            self._logger = get_task_logger(self.name)
        return self._logger

    def run(self, auth_id, redis_client=None, debug=False,
            fs_prefix='/tmp/snowcat'):
        self.logger.info('finalizing {0}'.format(auth_id))
        if redis_client is None:
            redis_client = redis.StrictRedis()

        FINISHED_FLAG_TTL = 7 * 24 * 60 * 60
        redis_client.setex('{0}:finished'.format(auth_id), FINISHED_FLAG_TTL, True)

        if bool(redis_client.get('snowcat_debug')):
            return

        logger = get_task_logger('stream_finalizer')
        logger.debug('Stream finalizer started for user {0}'.format(auth_id))

        keys = []
        cursor, first = 0, True
        while int(cursor) != 0 or first:
           first = False
           cursor, data = redis_client.scan(
               cursor,
               match='{0}:*'.format(auth_id)
           )

           # delete all keys except those related to locks or finished flags.
           keys.extend(
               [d for d in data
                if not d.endswith(':lock') and not d.endswith(':finished')]
           )

        if keys:
           redis_client.delete(*keys)

        if not debug:
           # remove queues
           rmtree(os.path.join(fs_prefix, auth_id))
