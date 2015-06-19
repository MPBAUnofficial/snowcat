from celery import Task
from celery.utils.log import get_task_logger
import redis
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
