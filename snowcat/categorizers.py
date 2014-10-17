from abc import abstractmethod
import pickle
from celery import Task
import redis
from utils.redis_utils import PersistentObject, RedisList
from decorators import singleton_task
import time

redis_client = redis.StrictRedis()


def get_all_categorizers(celeryapp):
    res = set()

    for cat in celeryapp.tasks.itervalues():
        if isinstance(cat, Categorizer):
            res.add(cat)

    return list(res)


def get_root_categorizers(celeryapp):
    res = set()

    for cat in get_all_categorizers(celeryapp):
        if not len(cat.DEPENDENCIES):
            res.add(cat)

    return list(res)


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

    @property
    def children(self):
        if not hasattr(self, '_children') or self._children is None:
            self._children = set()

            for cat in get_all_categorizers(self.app):
                if self.name in cat.DEPENDENCIES:
                    self._children.add(cat.name)

        return list(self._children)

    def is_root_categorizer(self):
        return not len(self.DEPENDENCIES)

    def call_children(self, auth_id):
        """ Call all the tasks which depend on this one.
        """
        children = self.children

        for cat in children:
            task = self.app.tasks[cat]
            task.run_if_not_already_running(auth_id)

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
                '{0}:*'.format(self.gen_key(auth_user_id))):
            redis_client.delete(item)


class LoopCategorizer(Categorizer):
    abstract = True

    s = None

    INPUT_QUEUE = None
    CHECKPOINT_FREQUENCY = 60  # a minute
    DEFAULT_S = {}

    PREFETCH = True
    BUFFER_LENGTH = 10

    rl = RedisList()

    @abstractmethod
    def initialize(self, user):
        pass

    def _initialize(self, user):
        self.initialize(user)
        children = self.children

        # it's a utterly inefficient way, but it works
        for c in get_all_categorizers(self.app):
            if c.name in children:
                c._initialize(user)

    @singleton_task
    def run(self, user):
        # default data to put into persistent storage
        def_s = {'idx': 0, 'last_save': 0.0}
        def_s.update(self.DEFAULT_S)

        self.s = PersistentObject(
            self.gen_key(user),
            default=def_s
        )

        if self.is_root_categorizer():
            self._initialize(user)

        rl = RedisList()

        buf = {}

        self.pre_run()

        while True:
            if self.PREFETCH:
                # try to optimize redis latency by fetching multiple data and
                # caching it
                raw_item = self.rlindex_buffered(
                    '{0}:{1}'.format(self.INPUT_QUEUE, user),
                    self.s.idx,
                    buf,
                    rl
                )
            else:
                raw_item = rl.lindex(
                    '{0}:{1}'.format(self.INPUT_QUEUE, user),
                    self.s.idx
                )

            item = pickle.loads(raw_item) if raw_item is not None else None

            time_since_last_save = time.time() - self.s.last_save

            if item is None or time_since_last_save > self.CHECKPOINT_FREQUENCY:
                self.call_children(user)
                self.checkpoint(user)
                self.s.last_save = time.time()
                self.s.save()

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
        res = buf.get(key, {}).get(index, None)
        if res is None:
            if rl is None:
                rl = RedisList()

            index_range = range(index - self.BUFFER_LENGTH,
                                index + self.BUFFER_LENGTH)
            buf[key] = dict(zip(index_range, rl.mlindex(key, *index_range)))

        res = buf[key][index]
        return res

    @abstractmethod
    def process(self, user, item):
        pass

    @abstractmethod
    def checkpoint(self, user):
        pass

    def pre_run(self):
        pass

    def post_run(self):
        pass
