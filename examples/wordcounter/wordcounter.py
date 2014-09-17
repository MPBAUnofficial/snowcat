from flask import Flask, request
from snowcat.core import Topology
from celeryapp import celeryapp
from categorizers.wordcounter import WordCounter
from categorizers.wordsplitter import WordSplitter

app = Flask(__name__)
t = None


@app.route("/", methods=['POST'])
def process_data():
    if request.method == 'POST':  # should not be necessary, but still...
        user = request.form['user']
        data = request.form['data']
        # print data
        t.add_data(user, data)
        return 'ok'  # Too lazy to handle errors. TODO: handle errors!!


def run_snowcat():
    global t
    t = Topology('wordcounter', celeryapp, [WordSplitter, WordCounter])

    errors = t.init_topology()

    if errors:
        print 'The following errors were encountered when trying to start SnowCat:'
        for e in errors:
            print '* {0}'.format(e)
        print

    else:
        app.run()

if __name__ == '__main__':
    run_snowcat()
