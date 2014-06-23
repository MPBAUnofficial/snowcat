from __future__ import absolute_import

from celery import Celery

celeryapp = Celery('categorizers')
celeryapp.config_from_object('categorizers.celeryconfig')

if __name__ == '__main__':
    celeryapp.start()
