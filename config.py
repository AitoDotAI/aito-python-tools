from pathlib import Path
import logging
import datetime
import sys


def root_path():
    return Path(__file__).parent


def set_up_logger(log_file_path: Path = None, logging_level: int = logging.INFO):
    """
    Wrapper for logging basic config
    :param log_file_path: path to log file. if not defined then use stderr
    :param log_folder: Path from root to log folder
    :param logging_level: The logging levels
    """
    if log_file_path:
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        if log_file_path.exists():
            log_file_name = log_file_path.name + str(datetime.datetime.now().isoformat(' ', 'seconds'))
            log_file_path = str(log_file_path.parent / log_file_name)

    logging.basicConfig(filename=log_file_path, level=logging_level,
                        format='%(asctime)-5s %(name)-5s %(levelname)-10s %(message)s',
                        datefmt='%H:%M:%S')
