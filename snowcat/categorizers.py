from abc import abstractmethod
from celery import Task
import msgpack
from utils.redis_utils import PersistentObject, SimpleKV
from decorators import singleton_task
from lockfile import LockFile
import time
import os
import redis


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

    redis_client = redis.StrictRedis()

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

    def shared_key(self, user, key):
        """
        Generate a unique key that refers to the user and not the
        categorizer itself (i.e. for communication between categorizers).
        """
        return '{0}:{1}'.format(user, key)

    def _initialize(self, user):
        pass

    @property
    def children(self):
        """ Return list with the names of the children of the categorizer """
        if not hasattr(self, '_children') or self._children is None:
            self._children = set()

            for cat in get_all_categorizers(self.app):
                if self.name in cat.DEPENDENCIES:
                    self._children.add(cat.name)

        return list(self._children)

    def is_root_categorizer(self):
        """ Return True if categorizer does not depend on other categorizers """
        return not len(self.DEPENDENCIES)

    def call_children(self, auth_id):
        """ Call all the categorizers which depend on this one. """
        children = self.children

        for cat in children:
            task = self.app.tasks[cat]
            task.run_if_not_already_running(auth_id)

    @singleton_task
    def run(self, user):
        pass

    def is_running(self, user):
        """ Return True if the categorizer is running """
        lock_key = self.gen_key(user, 'lock')

        if self.redis_client.get(lock_key) is None:
            return False
        return True

    def run_if_not_already_running(self, user, *args, **kwargs):
        if not self.is_running(user):
            self.delay(user, *args, **kwargs)

    def cleanup(self, user):
        """ Delete data related to this categorizer """
        keys = []
        cursor, first = 0, True
        while int(cursor) != 0 or first:
            first = False
            cursor, data = self.redis_client.scan(
                cursor,
                match='{0}:*'.format(self.gen_key(user))
            )
            keys.extend([d for d in data if not d.endswith(':lock')])

        if keys:
            self.redis_client.delete(*keys)


class LoopCategorizer(Categorizer):
    abstract = True

    s = None
    kv = None

    FSQUEUE_PREFIX = '/tmp/snowcat/'
    INPUT_QUEUE = None
    CHECKPOINT_FREQUENCY = 60  # in seconds
    DEFAULT_S = {}
    CALL_CHILDREN = True

    BUFFER_LENGTH = 10

    def queue_dir(self, auth_id, queue=None):
        if queue is None:
            queue = self.INPUT_QUEUE

        return os.path.join(self.FSQUEUE_PREFIX, str(auth_id), queue, 'queue')

    def initialize(self, user):
        """ Runs every time the categorizer is waked up.
        Note that it may be runned more than once.
        """
        pass

    def _initialize(self, user):
        """ Recursively call initialization method of all children.
        """
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

    @staticmethod
    def save_chunk_fs(data, queue_dir):
        """ Save a chunk of data on the file system.
        Data will be serialized as messagepack.
        """
        # todo: give option to set index manually
        # todo: clean the directory after a bit of time
        try:
            ls = os.listdir(queue_dir)
        except OSError:
            if not os.path.exists(queue_dir):
                os.makedirs(queue_dir)
                ls = []
            else:
                return False

        mx = 0
        for s in ls:
            try:
                num = int(s)
            except ValueError:
                continue
            mx = max(mx, num)

        file_path = os.path.join(queue_dir, str(mx + 1))
        lock = LockFile(file_path)
        with lock, open(file_path, 'wb') as f:  # todo: lock timeout?
            f.write(msgpack.dumps(data))
        return True

    def _fill_buffer(self, user, chunk_num):
        """ Fill the buffer with the data in <chunk_num>-th file.
        Return False if the file does not exist.
        """
        file_path = os.path.join(self.queue_dir(user),
                                 str(chunk_num))

        if os.path.exists(file_path):
            lock = LockFile(file_path)
            with lock, open(file_path, 'rb') as f:  # todo: lock timeout
                self.s.cat__buf = msgpack.unpack(f)
                self.s.cat__buf_offset = self.s.idx
                self.s.cat__chunk = chunk_num
                return True
        return False

    def bufget(self, user, _idx, rec=True):
        # fill buffer if it is empty
        if self.s.cat__buf_offset is None or self.s.cat__buf is None:
            res = self._fill_buffer(user, self.s.cat__chunk + 1)
            if not res or not rec:
                return None

        # must be a function since buf_offset will change if buffer is filled
        buf_idx = lambda: _idx - self.s.cat__buf_offset

        if buf_idx() >= len(self.s.cat__buf):
            if not self._fill_buffer(user, self.s.cat__chunk + 1):
                return None

        if buf_idx() < 0:
            return None

        return self.s.cat__buf[buf_idx()]

    @singleton_task
    def run(self, user):
        # if the categorizer is not active, just call his children
        if not self.is_active(user):
            if self.CALL_CHILDREN:
                self.call_children(user)
            return

        # default data to put into persistent storage
        def_s = {
            'idx': 0,
            'last_save': 0.0,
            'loop': True,
            'cat__chunk': 0,
            'cat__buf': None,
            'cat__buf_offset': None
        }
        def_s.update(self.DEFAULT_S)

        self.kv = SimpleKV(user)  # global keyvalue storage

        # local keyvalue storage
        self.s = PersistentObject(
            self.gen_key(user),
            default=def_s
        )
        self.s.loop = True

        if self.is_root_categorizer():
            self._initialize(user)

        self.pre_run(user)

        while self.s.loop:
            item = self.bufget(user, self.s.idx)

            time_since_last_save = time.time() - self.s.last_save

            if item is None or time_since_last_save > self.CHECKPOINT_FREQUENCY:
                if self.CALL_CHILDREN:
                    self.call_children(user)

                self.checkpoint(user)
                self.s.last_save = time.time()

            if item is None:
                break

            self.process(user, item)

            self.s.idx += 1

        self.s.save()

        self.post_run(user)

        # todo: a different, asynchronous task to check if new data is available
        #       since now there is still a little time frame where
        #       race conditions may occur.
        if self.s.loop:
            # check if new data has been added in the meantime
            item = self.bufget(user, self.s.idx)
            if item is not None:
                self.apply_async(countdown=2, args=(user,))

        self.s = None

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
