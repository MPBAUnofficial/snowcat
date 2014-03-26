import time
import socket
import json
import sys
import random
import requests
import psycopg2
from datetime import datetime

from local_db_settings import DB_SETTINGS


def build_query(auth_user_id):
    return 'select * from user_location_track  where auth_user_id = {0}' \
           ' order by ts'.format(auth_user_id)


def print_track(points_list):
    for p in points_list:
        print 'POINT x: {0}; y: {1}; z:{2};'.format(p['x'], p['y'], p['z'])


def run_fake_client(auth_user_id):
    # time.sleep(0.5)

    # data = {
    #     'op': 'categorize',
    #     'data': [
    #         {
    #             'id': random.randint(40000, 50000),
    #             'auth_user_id': 23,
    #             'ts': time.time(),
    #             'type': random.randint(1, 4),
    #             'x': 11.0586731075688,
    #             'y': 46.0384978349166,
    #             'z': 24,
    #             'm': 1234,
    #             'track_id': None,
    #             'sensor': None
    #         }
    #     ]
    # }

    with psycopg2.connect(**DB_SETTINGS) as conn:
        with conn.cursor() as cur:
            cur.execute(build_query(auth_user_id))

            # convert python's datetime to timestamp
            dt_to_ts = lambda dt: \
                time.mktime(dt.timetuple()) + dt.microsecond / 1E6

            data_all = [
                dict(id=t[0], auth_user_id=t[1], ts=dt_to_ts(t[2]), type=t[3],
                     x=t[4], y=t[5], z=t[6], m=t[7], track_id=t[8], sensor=t[9])
                for t in cur.fetchall()
            ]

    block_length = 10
    for i in range(0, len(data_all), block_length):
        # send the data three elements at a time,
        # in this way we'll test the system in a better way
        # (in the real world, a track should be sent in a single great block)
        data = data_all[i:i+block_length]
        payload = {'json_data': json.dumps({
            'op': 'categorize',
            'data': data
        })
        }

        requests.post('http://localhost:5000/', data=payload)
        print_track(data)

        time.sleep(0.5)


if __name__ == '__main__':
    auth_id = 32
    if len(sys.argv) == 2:
        auth_id = sys.argv[1]
    run_fake_client(auth_id)
