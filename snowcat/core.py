from tasks import AddData, InitTopology


class TopologyInitializedException(Exception):
    """
    Cannot make changes to the topology when it has already been initialized
    """
    pass


class Topology(object):
    def __init__(self, name, app, categorizers=None):
        if categorizers is None:
            categorizers = []

        self.name = name
        self.categorizers = map(lambda t: t(), categorizers)
        self.app = app
        self._add_data = AddData()
        self._add_data.bind(self.app)
        self._init_topology = InitTopology()
        self._init_topology.bind(self.app)
        self.initialized = False

    def add_categorizers(self, categorizers):
        if self.initialized:
            raise TopologyInitializedException

        names = [cat.name for cat in self.categorizers]

        for cat in categorizers:
            if cat.name not in names:
                self.categorizers.append(cat())

    def add_categorizer(self, categorizer):
        self.add_categorizers(categorizer)

    def add_data(self, user, data, redis_queue=None):
        return self._add_data.delay(self, user, data, redis_queue)

    def init_topology(self):
        res = self._init_topology.delay(self).get()
        if not res:
            self.initialized = True
        return res

