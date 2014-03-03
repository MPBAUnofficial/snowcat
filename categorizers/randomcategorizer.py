from __future__ import absolute_import

import time
import pickle

from .celery import celeryapp
from .base import Categorizer, redis_client


class RandomCategorizer(Categorizer):
    @classmethod
    @celeryapp.task
    def _run(cls, auth_id):
        time.sleep(1)
        while True:
            raw_data = redis_client.lpop(cls.gen_key(auth_id))
            if raw_data is None:
                break
            data = pickle.loads(raw_data)

            # DO SOMETHING WITH THE DATA
            # GARBAGE COLLECTOR
            # (todo: catalogare i punti spazzatura, fuori dalla traccia)
            time.sleep(1)

