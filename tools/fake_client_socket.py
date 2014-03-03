import time
import socket
import json
import sys
import random

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

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 6767))
    s.send(json.dumps(data))
    result = json.loads(s.recv(1024))
    print result
    s.close()
