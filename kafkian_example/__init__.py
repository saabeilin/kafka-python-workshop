import logging
from logging.config import dictConfig

LOG_FORMAT_STRING = '%(asctime)s\t%(levelname)s\t[%(name)s in %(funcName)s]\t%(message)s'

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'console': {
            'format': LOG_FORMAT_STRING
        },
    },
    'handlers': {
        'console': {
            'level': logging.DEBUG,
            'class': 'logging.StreamHandler',
            'formatter': 'console'
        },
    },
    'loggers': {
        # The default
        '': {
            'handlers': ['console'],
            'level': logging.DEBUG,
            'propagate': True,
        },
        'kafkian': {
            'handlers': ['console', ],
            'level': logging.DEBUG,
            'propagate': False,
        },
        'librdkafka': {
            'handlers': ['console', ],
            'level': logging.DEBUG,
            'propagate': False,
        },
    },
}
dictConfig(LOGGING)
