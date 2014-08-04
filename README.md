SnowCat
=======

Real time tracks categorizer for SKILO

Requirements
------------
SnowCat requirements:

* celery
* redis
* Flask (for the flask version)

Categorizers requirements:

* utm

Usage
-----
Start the message broker (ie. Redis)

`redis-server`

Start Celery (from the `snowcat` directory):

`celery -A src.categorizers worker`

Start SnowCat server

`python main.py`

SnowCat is now running.
You can test it with the `fake-client` tool

`python bin/fake_client.py`
