import logging

import config


def initialise_logger():
    log = logging.getLogger(config.app)
    log.setLevel(config.Logging.level)

    # create a file handler
    handler = logging.FileHandler(config.Logging.persistence_path)
    handler.setLevel(config.Logging.level)

    # create a logging format
    formatter = logging.Formatter(config.Logging.message_format)
    handler.setFormatter(formatter)

    # add the handlers to the log
    log.addHandler(handler)
    return log

log = initialise_logger()
