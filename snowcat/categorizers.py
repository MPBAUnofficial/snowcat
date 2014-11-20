from abc import abstractmethod
from celery import Task
import msgpack
import redis
from utils.redis_utils import PersistentObject, RedisList
from decorators import singleton_task
import time


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


def get_categorizer_by_name(celeryapp, name):
    # todo: maybe it would be better to do celeryapp.tasks[name]
    for cat in get_all_categorizers(celeryapp):
        if cat.name == name:
            return cat
    raise IndexError('{0} is not a valid categorizer name'.format(name))


class Categorizer(Task):
    abstract = True
    name = 'Categorizer'

    DEPENDENCIES = []

    rl = RedisList()

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

    def _initialize(self, user):
        pass

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

        if self.rl.redis_client.get(lock_key) is None:
            return False
        return True

    def run_if_not_already_running(self, user, *args, **kwargs):
        if not self.is_running(user):
            self.delay(user, *args, **kwargs)

    def cleanup(self, user):
        """ Delete from redis every entry related to this categorizer """
        r = self.rl.redis_client

        keys = []
        cursor, first = '0', True
        while cursor != '0' or first:
            cursor, data = r.scan(
                cursor,
                match='{0}:*'.format(self.gen_key(user))
            )
            keys.extend(data)

        r.delete(*keys)


class LoopCategorizer(Categorizer):
    abstract = True

    s = None

    INPUT_QUEUE = None
    CHECKPOINT_FREQUENCY = 60  # in seconds
    DEFAULT_S = {}
    CALL_CHILDREN = True

    PREFETCH = True
    BUFFER_LENGTH = 10

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

    def is_active(self, user):
        """
        Override this method if the categorizer will be active at some time only
        (i.e. the garbage collector which analyses the first n points only).
        If False is returned, the categorizer will just call its children.
        """
        return True

    @singleton_task
    def run(self, user):
        # if the categorizer is not active, just call his children
        if not self.is_active(user):
            print "returning 0"
            if self.CALL_CHILDREN:
                self.call_children(user)
            print "returning"
            return

        # default data to put into persistent storage
        def_s = {'idx': 0, 'last_save': 0.0, 'loop': True}
        def_s.update(self.DEFAULT_S)

        self.s = PersistentObject(
            self.gen_key(user),
            default=def_s
        )

        if self.is_root_categorizer():
            self._initialize(user)

        rl = RedisList()

        buf = {}

        self.pre_run(user)

        while True:
            if self.PREFETCH:
                # try to optimize redis latency by fetching multiple data and
                # caching it
                item = self.rlindex_buffered(
                    '{0}:{1}'.format(self.INPUT_QUEUE, user),
                    self.s.idx,
                    buf,
                    rl
                )
            else:
                item = rl.lindex(
                    '{0}:{1}'.format(self.INPUT_QUEUE, user),
                    self.s.idx
                )

            time_since_last_save = time.time() - self.s.last_save

            if item is None or time_since_last_save > self.CHECKPOINT_FREQUENCY:
                if self.CALL_CHILDREN:
                    self.call_children(user)

                self.checkpoint(user)
                self.s.last_save = time.time()

            if item is None:
                break

            self.process(user, item)

            if not self.s.loop:
                self.s.loop = True
                break

            self.s.idx += 1

        self.s.save()

        idx = self.s.idx

        self.post_run(user)
        del self.s

        # check if new data has been added in the meantime
        item = rl.lindex('{0}:{1}'.format(self.INPUT_QUEUE, user), idx)
        if item is not None:
            self.apply_async(countdown=2, args=(user,))

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

    def pre_run(self, user):
        pass

    def post_run(self, user):
        pass
