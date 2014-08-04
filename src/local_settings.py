DEBUG = True
REDIS_DB = 0

DATABASES = {
    'skilo_dev': {
        'host': 'geopg',
        'port': '50003',
        'database': 'skilo_dev',
        'user': 'skilo',
        'password': 'lover@ski!!',
        }
}


class CeleryConfig(object):
    ## Broker settings
    # BROKER_URL = 'amqp://guest:guest@localhost:5672//'
    BROKER_URL = 'redis://'

    # List of modules to import when celery starts.
    # CELERY_IMPORTS = ('base', 'stats_categorizer')

    ## Using the database to store task state and results.
    CELERY_RESULT_BACKEND = 'redis://'
