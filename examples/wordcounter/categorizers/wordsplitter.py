from snowcat.categorizers import LoopCategorizer


class WordSplitter(LoopCategorizer):
    name = 'WordSplitter'

    DEPENDENCIES = []
    CHECKPOINT_FREQUENCY = 10  # ten seconds
    INPUT_QUEUE = 'Stream'
    DEFAULT_S = {'buf': []}

    SEPARATORS = (' ', ';', ',', '\n', '\t')

    def initialize(self, user):
        self.rl.mark('Stream:{0}'.format(user), self.name)

    def process(self, user, val, *args, **kwargs):
        char = val

        if char in self.SEPARATORS and self.s.buf != []:
            self.rl.rpush('Words:{0}'.format(user), ''.join(self.s.buf).strip())
            self.s.buf = []

        if char not in self.SEPARATORS:
            self.s.buf.append(char)

    def checkpoint(self, user):
        self.rl.mark('Stream:{0}'.format(user), self.name, self.s.idx)