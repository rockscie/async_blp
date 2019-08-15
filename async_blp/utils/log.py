"""
Shared logger
"""
import logging


def set_logger(log_level: logging.WARNING):
    """
    Set logging level for the package
    """
    logger = logging.getLogger('async_blp')
    logger.setLevel(log_level)

    log_format = ("%(levelname)-8s %(asctime)s %(name)-15s %(threadName)s:"
                  " %(message)s")

    formatter = logging.Formatter(log_format)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


def get_logger():
    """
    Get shared logger
    """
    logger = logging.getLogger('async_blp')
    return logger
