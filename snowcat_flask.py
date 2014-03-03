from __future__ import absolute_import

from categorizers import all_categorizers
import local_settings

import json
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
        return 'ok'


@app.route("/test")
def test():
    for cat in all_categorizers:
        cat.run.delay([])
    return 'ok'

if __name__ == "__main__":
    app.run()