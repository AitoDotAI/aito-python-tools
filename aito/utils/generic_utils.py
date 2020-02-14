import datetime
import logging
from pathlib import Path
import time


def root_path():
    return Path(__file__).parent.parent.parent


def set_up_logger(log_file_path: Path = None, logging_level: int = logging.INFO):
    """
    Wrapper for logging basic config
    :param log_file_path: path to log file. if not defined then use stderr
    :param logging_level: The logging levels
    """
    if log_file_path:
        if log_file_path.exists():
            log_file_path = str(log_file_path)
        else:
            log_file_path = None
    logging.basicConfig(filename=log_file_path, level=logging_level,
                        format='%(asctime)s %(name)-20s %(levelname)-10s %(message)s',
                        datefmt="%Y-%m-%dT%H:%M:%S%z")
    logging.Formatter.converter = time.gmtime
    logging.VERBOSE = 5
    logging.addLevelName(logging.VERBOSE, "VERBOSE")
    logging.Logger.verbose = lambda inst, msg, *args, **kwargs: inst.log(logging.VERBOSE, msg, *args, **kwargs)
    logging.verbose = lambda msg, *args, **kwargs: logging.log(logging.VERBOSE, msg, *args, **kwargs)

