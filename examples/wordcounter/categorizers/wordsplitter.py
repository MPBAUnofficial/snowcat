from snowcat.categorizers import LoopCategorizer
from snowcat.utils.redis_utils import RedisList


class WordSplitter(LoopCategorizer):
    name = 'WordSplitter'

    DEPENDENCIES = []
    CHECKPOINT_FREQUENCY = 1  # a second
    QUEUE = 'Stream'
    DEFAULT_S = {'buf': []}
    PREFETCH = False

    rl = RedisList()

    def process(self, user, val, *args, **kwargs):
        char = val

        if char == ' ' and self.s.buf != []:
            self.rl.rpush('Words:{0}'.format(user), ''.join(self.s.buf).strip())
            self.s.buf = []

        if char != ' ':
            self.s.buf.append(char)
