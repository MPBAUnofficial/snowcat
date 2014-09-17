from celery import Task
import redis
from utils.redis_utils import RedisList


class InitTopology(Task):
    def run(self, topology, *args, **kwargs):
        r = redis.StrictRedis()

        errors = []

        # check for errors
        for cat in topology.categorizers:
            # make sure every categorizer has a valid name
            if not cat.name:
                errors.append('{0} is not a valid name'.format(cat.name))

        # delete previously stored data
        r.delete('root_categorizers')
        for k in r.keys('*_children'):
            r.delete(k)

        for cat in topology.categorizers:
            # todo: check for cycles

            cat.set_topology(topology)
            cat.bind(topology.app)

            if len(cat.DEPENDENCIES) == 0:
                print 'root categorizer: ' + str(cat.name)
                r.sadd('root_categorizers', str(cat.name))
            else:
                for dep in cat.DEPENDENCIES:
                    assert dep in [c.name for c in topology.categorizers]

                    # map every categorizer to the tasks which depend on itself
                    r.sadd('{0}_children'.format(dep), cat.name)

        return errors


class AddData(Task):
    queue = 'add_data'

    def run(self, topology, user, data, redis_queue=None):
        if redis_queue is None:
            redis_queue = 'Stream'

        r = redis.StrictRedis()

        # lock = r.lock('add_data_{0}'.format(user), timeout=LOCK_EXPIRE)
        # lock.acquire()

        rl = RedisList(redis_client=r)
        rl.rpush('{0}:{1}'.format(redis_queue, user), *data)

        # lock.release()

        root_categorizers = r.smembers('root_categorizers')

        for cat in topology.categorizers:
            if cat.name in root_categorizers:
                cat.run_if_not_already_running(user)

        return True

    def apply_async(self, *args, **kwargs):
        if 'queue' not in kwargs and self.queue:
            kwargs['queue'] = self.queue

        return super(AddData, self).apply_async(*args, **kwargs)

    def delay(self, *args, **kwargs):
        return self.apply_async(args, kwargs)
