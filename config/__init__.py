""" Module containing all the config for the app
"""

import os


app = 'render_stats'


class DataStore(object):
    """ Config class for data store
    """
    # "\d{4}" doesn't works with glob therefore multiple [0-9]
    filename_pattern = 'renders_{year}-{month}-{date}.csv'.format(
        year='[0-9]' * 4, month='[0-9]' * 2, date='[0-9]' * 2)


class Columns(object):
    """ Config class for persistent store scheme's columns.
    """
    uid = 'uid'
    app = 'app'
    renderer = 'renderer'
    frames = 'frames'
    success = 'success'
    elapsed_time = 'elapsed_time'
    maxram = 'maxram'
    maxcpu = 'maxcpu'

    all_ = [
        (uid, str),
        (app, str),
        (renderer, str),
        (frames, int),
        (success, str),
        (elapsed_time, int),
        (maxram, float),
        (maxcpu, float),
    ]


class Arguments(object):
    """ Configs for user arguments.
    """
    args = {
        'dir': ('logs_dir', 'Directory to get the render logs.', '?', os.getcwd),
        'filters': {
            'description': 'Filter the render records.',
            'arguments': [
                ('app', 'a', 'Filter by app.'),
                ('renderer', 'r', 'Filter by renderer.'),
                ('failed', 'f', 'Include failed renders.', 'store_true'),
            ]
        },
        'aggregators': {
            'description': 'Calculate stats of renders.',
            'arguments': [
                ('avgtime', 'at', 'Find the average elpased time for renders.', 'store_true'),
                ('avgcpu', 'ac', 'Find the average cpu usage of renders.', 'store_true'),
                ('avgram', 'ar', 'Find the average ram usage of renders.', 'store_true'),
                ('maxram', 'mr', 'Find the maximum ram usage of renders.', 'store_true'),
                ('maxcpu', 'mc', 'Find the maximum cpu usage of renders.', 'store_true'),
                ('summary', 's', 'Output the summary by printing avg_time avg_cpu avg_ram max_cpu.', 'store_true'),
            ]
        }
    }


class Output(object):
    """ Config for output result.
    """
    display_delimiter = '\n'
    conversion = {
        'avgtime': lambda x: x * .001,  # milliseconds to seconds
    }


class Concurrency(object):
    """ Config for concurrency in app.
    """
    retriever_threads = 2
    filter_threads = 2
    aggregator_threads = 2
    prefetch_count = 10
    timeout = .3


class Cache(object):
    """ Config for caching
    """
    # relative to parent root not from here
    persistence_path = os.path.abspath('./_logs/cache.cache')


class Logging(object):
    import logging
    level = logging.INFO
    message_format = '%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s'
    persistence_path = os.path.abspath('./_logs/log.log')
