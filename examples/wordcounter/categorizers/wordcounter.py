import redis
from snowcat.categorizers import LoopCategorizer
from snowcat.utils.redis_utils import RedisList
import datetime


class WordCounter(LoopCategorizer):
    name = 'WordCounter'

    DEPENDENCIES = ['WordSplitter']
    CHECKPOINT_FREQUENCY = 10  # ten seconds
    INPUT_QUEUE = 'Words'
    DEFAULT_S = {}

    r = redis.StrictRedis()

    def process(self, user, val, *args, **kwargs):
        # segnale inizio stream
        if 'riju++' in str(val).strip().lower():
            with open('/tmp/snowcat_start', 'wb') as f:
                f.write(str(datetime.datetime.now()))

        # segnale fine stream
        if 'kenshiro' in str(val).strip().lower():
            with open('/tmp/snowcat_end', 'wb') as f:
                f.write(str(datetime.datetime.now()))

        self.r.zincrby('WordCount:{0}'.format(user), val, 1)

    def checkpoint(self, user):
        rl = RedisList()
        rl.mark('Words:{0}'.format(user), self.name, self.s.idx)