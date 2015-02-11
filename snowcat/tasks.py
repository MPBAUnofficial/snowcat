from celery import Task
import msgpack
import redis
from categorizers import get_root_categorizers, LoopCategorizer
import os


class BaseAddData(Task):
    queue = 'add_data'

    FSQUEUE_PREFIX = '/tmp/snowcat/'

    r = redis.StrictRedis()

    def gen_key(self, user, key=''):
        return '{0}:{1}{2}'.format(
            self.name,
            user,
            ':' + str(key) if key else ''
        )

    def keyval_set(self, auth_id, key, value):
        self.r.hset(self.gen_key(auth_id, 'kv'), key, msgpack.dumps(value))

    def keyval_get(self, auth_id, key, default=None):
        res = self.r.hget(self.gen_key(auth_id, 'kv'), key)
        if res is None:
            return default
        return msgpack.loads(res)

    def run(self, data, queue='Stream'):
        root_categorizers = get_root_categorizers(self.app)

        user = data['user']

        LoopCategorizer.save_chunk_fs(
            data['data'] if isinstance(data['data'], (tuple, list))
            else [data['data']],
            os.path.join(self.FSQUEUE_PREFIX, str(user), queue, 'queue')
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
