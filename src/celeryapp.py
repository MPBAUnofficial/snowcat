from celery import Celery
from local_settings import CeleryConfig
import categorizers

celeryapp = Celery()
celeryapp.config_from_object(CeleryConfig)

if __name__ == '__main__':
    celeryapp.start()
