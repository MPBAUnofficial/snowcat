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

    def initialize(self, user):
        self.rl.mark('Words:{0}'.format(user), self.name)

    def process(self, user, val, *args, **kwargs):
        r = self.rl.redis_client

        # segnale inizio stream
        if 'riju++' in str(val).strip().lower():
            with open('/tmp/snowcat_start', 'wb') as f:
                f.write(str(datetime.datetime.now()))

        # segnale fine stream
        if 'kenshiro' in str(val).strip().lower():
            with open('/tmp/snowcat_end', 'wb') as f:
                f.write(str(datetime.datetime.now()))

        r.zincrby('WordCount:{0}'.format(user), val, 1)

    def checkpoint(self, user):
        self.rl.mark('Words:{0}'.format(user), self.name, self.s.idx)