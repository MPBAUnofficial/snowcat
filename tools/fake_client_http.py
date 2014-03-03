import time
import socket
import json
import sys
import random
import requests


while 1:
    time.sleep(0.5)

    data = {
        'op': 'categorize',
        'data': [
            {
                'id': random.randint(40000, 50000),
                'auth_user_id': 23,
                'ts': time.time(),
                'type': random.randint(1, 4),
                'x': 11.0586731075688,
                'y': 46.0384978349166,
                'z': 24,
                'm': 1234,
                'track_id': None,
                'sensor': None
            }
        ]
    }

    payload = {'json_data': json.dumps(data)}
    requests.post('http://localhost:5000/', data=payload)