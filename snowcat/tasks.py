import pickle
from celery import Task
import redis
from utils.redis_utils import RedisList
from categorizers import get_root_categorizers


class AddData(Task):
    queue = 'add_data'

    def run(self, user, data, redis_queue=None):
        if redis_queue is None:
            redis_queue = 'Stream'

        r = redis.StrictRedis()

        rl = RedisList(redis_client=r)
        root_categorizers = get_root_categorizers(self.app)

        for cat in root_categorizers:
            rl.mark('Stream:{0}'.format(user), cat.name)

        rl.rpush('{0}:{1}'.format(redis_queue, user), *map(pickle.dumps, data))

        for cat in root_categorizers:
            cat.run_if_not_already_running(user)

        return True

    def apply_async(self, *args, **kwargs):
        if 'queue' not in kwargs and self.queue:
            kwargs['queue'] = self.queue

        return super(AddData, self).apply_async(*args, **kwargs)

    def delay(self, *args, **kwargs):
        return self.apply_async(args, kwargs)
