from __future__ import absolute_import
import snowcat
from categorizers import wordsplitter, wordcounter

from celery import Celery

celeryapp = Celery('wordcounter',
                   broker='amqp://',
                   backend='amqp',
                   include=['snowcat.tasks', 'categorizers'])

# Optional configuration, see the application user guide.
celeryapp.conf.update(
    CELERY_RESULT_BACKEND='amqp',
    CELERY_IGNORE_RESULT=False,
    CELERY_TASK_RESULT_EXPIRES=3600,
)


if __name__ == '__main__':
    celeryapp.start()
