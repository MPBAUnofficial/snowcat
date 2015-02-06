from snowcat.categorizers import LoopCategorizer


class WordSplitter(LoopCategorizer):
    name = 'WordSplitter'

    DEPENDENCIES = []
    CHECKPOINT_FREQUENCY = 10  # ten seconds
    INPUT_QUEUE = 'Stream'
    DEFAULT_S = {'buf': [], 'words': []}

    SEPARATORS = (' ', ';', ',', '\n', '\t')

    def process(self, user, val, *args, **kwargs):
        char = val

        if char in self.SEPARATORS and self.s.buf:
            self.s.words.append(''.join(self.s.buf).strip())
            self.s.buf = []

        if char not in self.SEPARATORS:
            self.s.buf.append(char)

    def checkpoint(self, user):
        if self.s.words:
            self.save_chunk_fs(self.s.words, self.queue_dir(user, 'Words'))
            self.s.words = []
            self.s.save()
