from snowcat.categorizers import LoopCategorizer
import datetime


class WordCounter(LoopCategorizer):
    name = 'WordCounter'

    DEPENDENCIES = ['WordSplitter']
    CHECKPOINT_FREQUENCY = 10  # ten seconds
    INPUT_QUEUE = 'Words'
    DEFAULT_S = {'words': {}}

    def process(self, user, val, *args, **kwargs):
        # segnale inizio stream
        if 'riju++' in str(val).strip().lower():
            with open('/tmp/snowcat_start', 'wb') as f:
                f.write(str(datetime.datetime.now()))

        # segnale fine stream
        if 'kenshiro' in str(val).strip().lower():
            with open('/tmp/snowcat_end', 'wb') as f:
                f.write(str(datetime.datetime.now()))

        current = self.s.words.get(val, 0)
        self.s.words[val] = current + 1

    def checkpoint(self, user):
        for k, v in self.s.words.iteritems():
            current = self.redis_client.hget('WordCount:{0}'.format(user), k)
            if current is None:
                current = 0
            self.redis_client.hset(
                'WordCount:{0}'.format(user), k, int(current) + v
            )

        self.s.words = {}
