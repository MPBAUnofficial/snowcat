from abc import abstractmethod
from celery import Task
from celery.canvas import chain
from celery.utils.log import get_task_logger
import msgpack
from utils.redis_utils import PersistentObject, SimpleKV
from decorators import singleton_task
from lockfile import LockFile
import time
import os
import redis


def get_stream_finalizers(celeryapp):
    from tasks import FinalizeStream  # avoid circular import

    # todo: define order of execution when multiple streamfinalizers
    # have been defined

    # if one or more streamfinalizers have been defined, ignore the default one
    # otherwise, use the default one only
    tasks = [t for t in celeryapp.tasks.itervalues()
             if isinstance(t, FinalizeStream)]
    if len(set(tasks)) == 1:
        return tasks

    tasks = [t for t in tasks if not t.name == FinalizeStream.name]

    return tasks


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


def initialize_categorizers(celeryapp, auth_id):
    """
    Initialize all the categorizers recursively starting from
    root categorizers.
    If another task is running categorizers initialization, just wait for it to
    finish and return.
    """
    kv = SimpleKV(auth_id)
    logger = get_task_logger('InitCategorizers<{0}>'.format(auth_id))

    def initialize_rec(categorizer):
        categorizer.initialize(auth_id)
        for cat in categorizer.children:
            initialize_rec(get_categorizer_by_name(celeryapp, cat))

    if kv.get('categorizers_initialization_finished', False):
        return

    if not kv.getset('categorizers_initialization_started', True, False):
        # if categorizers initialization didn't start yet
        logger.debug('starting initialization for {0}'.format(auth_id))
        for c in get_root_categorizers(celeryapp):
            initialize_rec(c)
        kv.categorizers_initialization_finished = True
        logger.debug('initialization finished for {0}'.format(auth_id))

    else:
        # otherwise, if initialization is running in another task, wait for
        # it to finish.
        logger.debug('waiting for initialization for {0}'.format(auth_id))
        while not \
                kv.get('categorizers_initialization_finished', False):
            time.sleep(0.5)
        logger.debug('initialization finished, stopped waiting for {0}'
                     .format(auth_id))


# TODO: merge Categorizer and LoopCategorizer
#       (or make it meaningful to keep them separated)
class Categorizer(Task):
    abstract = True
    name = 'Categorizer'

    DEPENDENCIES = []

    redis_client = redis.StrictRedis()

    @property
    def logger(self):
        if not hasattr(self, '_logger'):
            self._logger = get_task_logger(self.name)
        return self._logger

    @property
    def debug(self):
        if not hasattr(self, '_debug'):
            self._debug = bool(self.redis_client.get('snowcat_debug'))
        return self._debug

    def initialize(self, user):
        """
        Method called the first time the categorizer is called
        for a specific user.
        """
        pass

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

    def is_active(self, auth_id):
        """
        Override this method if the categorizer will be active at some time only
        (i.e. the garbage collector which analyses the first n points only).
        If False is returned, the categorizer will just call its children.
        """
        return True

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
        super(Categorizer, self).run()

    def is_running(self, user):
        """ Return True if the categorizer is running """
        lock_key = self.gen_key(user, 'lock')

        if self.redis_client.get(lock_key) is None:
            return False
        return True

    def has_finished(self, auth_id, categorizer=None):
        if self.redis_client.exists('{0}:finished'.format(auth_id)):
            return True

        if categorizer is not None:
            return self.redis_client.sismember(
                '{0}:finished_tasks'.format(auth_id), categorizer)

    def run_if_not_already_running(self, user, *args, **kwargs):
        if not self.is_running(user):
            self.delay(user, *args, **kwargs)

    def finalize(self, user, cleanup=True):
        """ Flag this categorizer as finished for this stream.
        If all the categorizers have finished processing this stream, call the
        stream finalizer task.
        :param user: Authentication id.
        :param cleanup: if cleanup is True and all the tasks have finished,
         cleanup. If False the cleanup has to be managed expressly.
        :return: True if all the other tasks finished, False otherwise.
        """
        p = self.redis_client.pipeline()
        k = '{0}:finished_tasks'.format(user)
        p.sadd(k, self.name)
        p.smembers(k)
        finished_tasks = p.execute()[1]

        if cleanup:
            self.cleanup(user)

        all_tasks = map(lambda c: c.name, get_all_categorizers(self.app))
        all_finished = not (set(all_tasks) - finished_tasks)
        self.logger.debug("All categorizers have finished processing stream {0}"
                          .format(user))
        if all_finished and cleanup:  # all tasks have finished processing
            self.finalize_stream(user)

        return all_finished

    def cleanup(self, user):
        """ Delete data related to this categorizer """
        if self.debug:
            return

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

    def finalize_stream(self, auth_id):
        """ Launch the stream finalizer tasks for this stream.
        """
        self.logger.debug("Finalizing stream {0}".format(auth_id))
        stream_finalizers = get_stream_finalizers(self.app)
        sf_tasks = chain(*[t.si(auth_id, debug=self.debug,
                                fs_prefix=self.FSQUEUE_PREFIX)
                           for t in stream_finalizers])
        sf_tasks.delay()


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

    @staticmethod
    def save_chunk_fs(data, queue_dir):
        """ Save a chunk of data on the file system.
        Data will be serialized as messagepack.
        """
        # todo: give option to set index manually
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
        with lock, open(file_path, 'wb') as f:
            f.write(msgpack.dumps(data))
        return True

    def _fill_buffer(self, auth_id, chunk_num):
        """ Fill the buffer with the data in <chunk_num>-th file.
        Return False if the file does not exist.
        """
        file_path = os.path.join(self.queue_dir(auth_id),
                                 str(chunk_num))

        if os.path.exists(file_path):
            lock = LockFile(file_path)
            with lock, open(file_path, 'rb') as f:  # todo: lock timeout
                # if file is empty, try again with the next one
                val = msgpack.unpack(f)
                if not val:
                    return self._fill_buffer(auth_id, chunk_num+1)

                # fill buffer
                self.s.cat__buf = val
                self.s.cat__buf_offset = self.s.idx
                self.s.cat__chunk = chunk_num
                return True
        return False

    def bufget(self, auth_id, _idx, rec=True):
        # fill buffer if it is empty
        if self.s.cat__buf_offset is None or self.s.cat__buf is None:
            res = self._fill_buffer(auth_id, self.s.cat__chunk + 1)
            if not res or not rec:
                return None

        # must be a function since buf_offset will change if buffer is filled
        buf_idx = lambda: _idx - self.s.cat__buf_offset

        if buf_idx() >= len(self.s.cat__buf):
            if not self._fill_buffer(auth_id, self.s.cat__chunk + 1):
                return None

        if buf_idx() < 0:
            return None

        return self.s.cat__buf[buf_idx()]

    @singleton_task
    def run(self, auth_id):
        super(LoopCategorizer, self).run(auth_id)

        self.logger.debug('{0} <run> started on user {1} and on app {2}'
                          .format(self.name, auth_id, str(self.app)))

        # launch categorizers initialization, if it hasn't been done already.
        initialize_categorizers(self.app, auth_id)

        # if the categorizer is not active, just call his children
        if not self.is_active(auth_id):
            if self.CALL_CHILDREN:
                self.call_children(auth_id)
            return

        # if the categorizer has already processed its stream, don't start it
        if self.has_finished(auth_id, self.name):
            self.logger.debug('Already finished, stopping now.')
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

        self.kv = SimpleKV(auth_id)  # global keyvalue storage

        # local keyvalue storage
        self.s = PersistentObject(
            self.gen_key(auth_id),
            default=def_s
        )
        self.s.loop = True

        self.pre_run(auth_id)

        while self.s.loop:
            item = self.bufget(auth_id, self.s.idx)

            time_since_last_save = time.time() - self.s.last_save

            if item is None or time_since_last_save > self.CHECKPOINT_FREQUENCY:
                if self.CALL_CHILDREN:
                    self.call_children(auth_id)

                self.checkpoint(auth_id)
                self.s.last_save = time.time()

            if item is None:
                break

            self.process(auth_id, item)

            self.s.idx += 1

        self.s.save()

        self.post_run(auth_id)

        # todo: a different, asynchronous task to check if new data is available
        #       since now there is still a little time frame where
        #       race conditions may occur.
        if self.s.loop:
            # check if new data has been added in the meantime
            item = self.bufget(auth_id, self.s.idx)
            if item is not None:
                self.apply_async(countdown=2, args=(auth_id,))

        self.s = None

    @abstractmethod
    def process(self, auth_id, item):
        pass

    @abstractmethod
    def checkpoint(self, auth_id):
        pass

    def pre_run(self, auth_id):
        pass

    def post_run(self, auth_id):
        pass
