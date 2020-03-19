import logging
import time
from pathlib import Path

ROOT_PATH = Path(__file__).parent.parent.parent


def logging_config(
        level=logging.INFO,
        log_format: str = '%(asctime)s %(name)-20s %(levelname)-10s %(message)s',
        log_date_fmt: str = '%Y-%m-%dT%H:%M:%S%z',
        **kwargs
):
    """Wrapper for logging basic config with a default format

    :param level: logging level
    :param log_format: logging format
    :param log_date_fmt: logging date format
    :param kwargs:
    :return:
    """
    logging.basicConfig(level=level, format=log_format, datefmt=log_date_fmt, **kwargs)
    logging.Formatter.converter = time.gmtime
