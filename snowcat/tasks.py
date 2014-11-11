from celery import Task
import redis
from utils.redis_utils import RedisList
from categorizers import get_root_categorizers


class BaseAddData(Task):
    queue = 'add_data'

    r = redis.StrictRedis()
    rl = RedisList(redis_client=r)

    def run(self, data, redis_queue=None):
        if redis_queue is None:
            redis_queue = 'Stream'

        root_categorizers = get_root_categorizers(self.app)

        user = data['user']

        for cat in root_categorizers:
            self.rl.mark('Stream:{0}'.format(user), cat.name)

        self.rl.rpush( '{0}:{1}'.format(redis_queue, user), *data)

        for cat in root_categorizers:
            cat.run_if_not_already_running(user)

        return True

    def apply_async(self, *args, **kwargs):
        if 'queue' not in kwargs and self.queue:
            kwargs['queue'] = self.queue

        return super(BaseAddData, self).apply_async(*args, **kwargs)

    def delay(self, *args, **kwargs):
        return self.apply_async(args, kwargs)
