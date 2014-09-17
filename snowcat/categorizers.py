from abc import abstractmethod
from celery import Task
import redis
from utils.redis_utils import PersistentObject, RedisList
from decorators import singleton_task
import time

redis_client = redis.StrictRedis()


class Categorizer(Task):
    abstract = True
    name = 'Categorizer'
    _topology = None

    DEPENDENCIES = []

    @property
    def topology(self):
        if self.__class__._topology is None:
            raise ValueError('No topology has been defined')
        return self.__class__._topology

    def set_topology(self, topology):
        self.__class__._topology = topology

    def gen_key(self, user, key=''):
        """ Generate a unique key to be used for indexing i.e. in Redis.
        Generated key will normally contain categorizer name and user id, and
        another key when defined.
        """
        return '{0}:{1}{2}'.format(
            self.name,
            user,
            ':' + str(key) if key else ''
        )

    def children(self):
        res = redis_client.smembers('{0}_children'.format(self.name))
        if res is None:
            return []
        return res

    def call_children(self, auth_id):
        """ Call all the tasks which depend on this one.
        """
        children = self.children()

        for cat in children:
            task = self.app.tasks[cat]
            task.run_if_not_already_running(auth_id)

        # for cat in self.topology.categorizers:
        #     if cat.name in children:
        #         cat.run_if_not_already_running(auth_id)

    @singleton_task
    def run(self, user):
        pass

    def is_running(self, user):
        lock_key = self.gen_key(user, 'lock')

        if redis_client.get(lock_key) is None:
            return False
        return True

    def run_if_not_already_running(self, user, *args, **kwargs):
        if not self.is_running(user):
            self.delay(user, *args, **kwargs)

    def close_session(self, auth_user_id):
        for item in redis_client.keys(
                '{0}*'.format(self.gen_key(auth_user_id))):
            redis_client.delete(item)


class LoopCategorizer(Categorizer):
    abstract = True

    s = None

    QUEUE = None
    CHECKPOINT_FREQUENCY = 60  # a minute
    DEFAULT_S = {}

    PREFETCH = True
    BUFFER_LENGTH = 10

    @singleton_task
    def run(self, user):
        # default data to put into persistent storage
        def_s = {'idx': 0, 'last_save': 0.0}
        def_s.update(self.DEFAULT_S)

        self.s = PersistentObject(
            self.gen_key(user),
            default=def_s
        )

        rl = RedisList()

        self.initialize()

        buf = {}

        self.pre_run()

        while True:
            if self.PREFETCH:
                # try to optimize redis latency by fetching multiple data and
                # caching it
                item = self.rlindex_buffered(
                    '{0}:{1}'.format(self.QUEUE, user),
                    self.s.idx,
                    buf
                )
            else:
                item = rl.lindex(
                    '{0}:{1}'.format(self.QUEUE, user),
                    self.s.idx
                )

            time_since_last_save = time.time() - self.s.last_save

            if item is None or time_since_last_save > self.CHECKPOINT_FREQUENCY:
                self.s.save()
                self.call_children(user)

            if item is None:
                break

            self.process(user, item)

            self.s.idx += 1

        self.s.save()
        del self.s

        self.post_run()

    def rlindex_buffered(self, key, index, buf, rl=None):
        """
        If the value is contained in the cache buffer, then return it.
        Otherwise, fetch the value from redis, caching some previous and
        following values in the buffer.

        self.BUFFER_LENGTH defines how long will be the window of buffered
        values.
        """
        if rl is None:
            rl = RedisList()

        res = buf.get(key, {}).get(index, None)
        if res is None:
            index_range = range(index - self.BUFFER_LENGTH,
                                index + self.BUFFER_LENGTH)
            buf[key] = dict(zip(index_range, rl.mlindex(key, *index_range)))

        res = buf[key][index]
        return res

    @abstractmethod
    def process(self, user, item):
        pass

    def initialize(self):
        pass

    def pre_run(self):
        pass

    def post_run(self):
        pass