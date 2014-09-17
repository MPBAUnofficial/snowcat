import redis
from snowcat.categorizers import LoopCategorizer
import datetime


class WordCounter(LoopCategorizer):
    name = 'WordCounter'

    DEPENDENCIES = ['WordSplitter']
    CHECKPOINT_FREQUENCY = 30  # half a minute
    QUEUE = 'Words'
    DEFAULT_S = {}
    PREFETCH = False

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
