from pathlib import Path
import logging
import datetime


def root_path():
    return Path(__file__).parent


def set_up_logger(log_file_name: str = 'log_file', log_folder: str = '.log', logging_level: int = logging.INFO):
    """
    Wrapper for logging basic config
    :param log_file_name: Name of the log file. The file will be put in root .log folder
    :param log_folder: Path from root to log folder
    :param logging_level: The logging levels
    """
    log_folder = (root_path() / log_folder)
    log_folder.mkdir(parents=True, exist_ok=True)
    if (log_folder / f"{log_file_name}.log").exists():
        log_file_name = log_file_name + str(datetime.datetime.now().isoformat(' ', 'seconds'))
    log_path = log_folder / (log_file_name + '.log')
    logging.basicConfig(filename=str(log_path), level=logging_level,
                        format='%(asctime)-5s %(name)-5s %(levelname)-10s %(message)s',
                        datefmt='%H:%M:%S')
