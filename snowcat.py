from __future__ import absolute_import

from categorizers import all_categorizers
import local_settings

import json
import redis
from flask import Flask, request
app = Flask(__name__)
app.debug = local_settings.DEBUG


@app.route("/", methods=['POST'])
def process_data():
    if request.method == 'POST':  # should not be necessary, but still...
        json_data = request.form['json_data']
        data = json.loads(json_data)
        if data['op'] == 'categorize':
            _d = data['data']
            for cat in all_categorizers:
                cat.add_data.delay(cat, _d)
        return 'ok'  # Too lazy to handle errors. TODO: handle errors!!


@app.route("/test")
def test():
    for cat in all_categorizers:
        cat.run.delay([])
    return 'ok'


def run_snowcat():
    r = redis.StrictRedis()

    # manage dependencies between tasks

    # delete previously stored data
    r.delete('root_categorizers')
    for k in r.keys('*_children'):
        r.delete(k)

    for cat in all_categorizers:
        if len(cat.DEPENDENCIES) == 0:
            r.sadd('root_categorizers', cat.ID)
        else:
            for dep in cat.DEPENDENCIES:
                assert dep in [cat.ID for cat in all_categorizers]

                # map every categorizer to the tasks which depend on itself
                r.sadd('{0}_children'.format(dep), cat.ID)

    app.run()

if __name__ == "__main__":
    run_snowcat()
