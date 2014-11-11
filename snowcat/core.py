from tasks import BaseAddData
from categorizers import get_all_categorizers


class Topology(object):
    def __init__(self, name, app, add_data=BaseAddData):
        self.name = name
        self.app = app
        self._add_data = add_data()
        self._add_data.bind(self.app)

    def add_data(self, data, redis_queue=None):
        return self._add_data.delay(data, redis_queue)

    def errors(self):
        errors = list()
        tasks_name = [t.name for t in get_all_categorizers(self.app)]

        for t in get_all_categorizers(self.app):
            if not t.name:
                errors.append(
                    '{0} is not a valid name for a categorizer'.format(t.name)
                )
            for dep in getattr(t, 'DEPENDENCIES', []):
                if not dep in tasks_name:
                    errors.append(
                        '{0} is not a registered categorizer'.format(dep)
                    )


