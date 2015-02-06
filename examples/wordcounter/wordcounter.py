from flask import Flask, request
from snowcat.core import Topology
from celeryapp import celeryapp

app = Flask(__name__)
app.debug = True
t = None


@app.route("/", methods=['POST'])
def process_data():
    if request.method == 'POST':  # should not be necessary, but still...
        # print data

        t.add_data({
            'user': request.form['user'],
            'data': list(request.form['data'])  # put one char at a time
        })
        return 'ok'  # Too lazy to handle errors. TODO: handle errors!!


def run_snowcat():
    global t
    t = Topology('wordcounter', celeryapp)

    errors = t.errors()

    if errors:
        print 'The following errors were encountered when trying to start SnowCat:'
        for e in errors:
            print '* {0}'.format(e)
        print

    else:
        app.run()

if __name__ == '__main__':
    run_snowcat()
