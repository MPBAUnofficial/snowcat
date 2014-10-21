import unittest

from nose.tools import assert_equal, assert_true, assert_false, assert_is_not_none

from snowcat.utils.redis_utils import RedisList, redis_mget, redis_mset


def test_mget_mset():
    redis_mset('__test_namespace', a=1, b=2, c=3)
    a, b, c, d = redis_mget('__test_namespace', ('a', 1), 'b', 'c', ['d', 4])
    assert_equal((a, b, c, d), (1, 2, 3, 4))


class RedisListTest(unittest.TestCase):
    L = '__redis_utils_test_list'

    def test_rpush(self):
        rl = RedisList()
        rl.delete(self.L)

        rl.rpush(self.L, 1, 2, 3, 4)

        assert_equal(rl.llen(self.L), 4)
        assert_equal(rl.get_offset(self.L), 0)
        assert_equal(rl.lrange(self.L, 0, -1), [1, 2, 3, 4])

    def test_llen(self):
        rl = RedisList()
        rl.delete(self.L)

        assert_equal(rl.llen(self.L), 0)

        rl.rpush(self.L, 1, 2, 3)
        rl.rpush(self.L, 4)
        assert_equal(rl.llen(self.L), 4)

        rl.remfirstn(self.L, 2)
        assert_equal(rl.llen(self.L), 2)

        rl.rpush(self.L, 1, 2, 3)
        assert_equal(rl.llen(self.L), 5)

    def test_lindex(self):
        rl = RedisList()
        rl.delete(self.L)

        rl.rpush(self.L, 0, 0, 1, 2, 3, 4)
        rl.remfirstn(self.L, 2)

        assert_equal(rl.lindex(self.L, 0), 1)
        assert_equal(rl.lindex(self.L, 1), 2)
        assert_equal(rl.lindex(self.L, 3), 4)
        assert_equal(rl.lindex(self.L, -1), 4)
        assert_equal(rl.lindex(self.L, -3), 2)
        assert_equal(rl.lindex(self.L, 100), None)
        assert_equal(rl.lindex(self.L, -100), None)

    def test_lrange(self):
        rl = RedisList()
        rl.delete(self.L)

        rl.rpush(self.L, 0, 0, 1, 2, 3, 4)
        rl.remfirstn(self.L, 2)

        assert_equal(rl.lrange(self.L, 0, 3), [1, 2, 3, 4])
        assert_equal(rl.lrange(self.L, 0, 100), [1, 2, 3, 4])
        assert_equal(rl.lrange(self.L, -100, 100), [1, 2, 3, 4])
        assert_equal(rl.lrange(self.L, 0, -1), [1, 2, 3, 4])
        assert_equal(rl.lrange(self.L, 0, 2), [1, 2, 3])
        assert_equal(rl.lrange(self.L, 100, 1000), [])
        assert_equal(rl.lrange(self.L, -100, -10), [])
        assert_equal(rl.lrange(self.L, 2, 1), [])

    def test_get_offset(self):
        rl = RedisList()
        rl.delete(self.L)

        assert_equal(rl.get_offset(self.L), 0)

        rl.rpush(self.L, 0, 0, 1, 2, 3, 4)
        rl.remfirstn(self.L, 2)
        assert_equal(rl.get_offset(self.L), 2)

        rl.rpush(self.L, 5, 6)
        rl.remfirstn(self.L, 2)
        assert_equal(rl.get_offset(self.L), 4)

    def test_delete(self):
        rl = RedisList()
        r = rl.redis_client
        rl.delete(self.L)

        rl.rpush(self.L, 1, 2, 3, 4)
        assert_true(r.exists(self.L))

        rl.delete(self.L)
        assert_false(r.exists(self.L))

    def test_remfirstn(self):
        rl = RedisList()
        rl.delete(self.L)

        rl.rpush(self.L, 1, 2, 3, 4)

        rl.remfirstn(self.L, -10)
        assert_equal(rl.llen(self.L), 4)

        rl.remfirstn(self.L, 1)
        assert_equal(rl.llen(self.L), 3)

        rl.remfirstn(self.L, 100)
        assert_equal(rl.llen(self.L), 0)

        assert_equal(rl.get_offset(self.L), 4)

    def test_lpop(self):
        rl = RedisList()
        rl.delete(self.L)

        rl.rpush(self.L, 1, 2, 3, 4)
        elem = rl.lpop(self.L)
        assert_equal(elem, 1)

        elem = rl.lpop(self.L)
        assert_equal(elem, 2)

        elem = rl.lpop(self.L)
        assert_equal(elem, 3)

        assert_equal(rl.llen(self.L), 1)

    def test_mark(self):
        rl = RedisList()
        rl.delete(self.L)

        rl.rpush(self.L, *range(20))
        rl.mark(self.L, 'foo', 0)
        assert_equal(rl.lrange(self.L, 0, -1), range(20))

        rl.mark(self.L, 'bar', 5)
        assert_equal(rl.lrange(self.L, 0, -1), range(20))

        rl.mark(self.L, 'foo', 10)
        assert_equal(rl.lrange(self.L, 0, -1), ([None]*5) + range(5, 20))

