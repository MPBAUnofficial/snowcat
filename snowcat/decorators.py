from functools import wraps
import redis

redis_client = redis.StrictRedis()
LOCK_EXPIRE = 60 * 60  # 1 hour


def singleton_task(func):
    """
    Decorator to make the task a pseudo-singleton.
    Enables a maximum of one task to be executed for each session
    for each categorizer (i.e. there can't be more than one RandomCategorizer
    running on session with auth_user_id 42).
    If the task is not able to acquire the lock, it will just fail silently.
    """

    @wraps(func)
    def _inner(self, auth_id, *args, **kwargs):
        # try to acquire lock
        lock_key = self.gen_key(auth_id, 'lock')

        lock = redis_client.lock(lock_key, timeout=LOCK_EXPIRE)
        have_lock = lock.acquire(blocking=False)

        if not have_lock:
            return False

        try:
            func(self, auth_id, *args, **kwargs)
        finally:
            lock.release()
            return True

    return _inner
