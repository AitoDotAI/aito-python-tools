import logging
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

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


def instance_url_problem(instance_url: str) -> Optional[str]:
    """why an instance URL cannot work, or None when it is well formed

    A URL without a scheme (``localhost:9005``) otherwise fails deep inside
    ``requests`` with "No connection adapters were found", which names neither
    the argument nor the fix.

    :param instance_url: the URL given to a client
    :type instance_url: str
    :rtype: Optional[str]
    """
    parsed = urlparse(instance_url)
    if parsed.scheme in ('http', 'https') and parsed.netloc:
        return None
    return (
        f"invalid instance URL '{instance_url}': it needs a scheme and a host, e.g. "
        f"'http://127.0.0.1:9005' for a local instance (`aito start` prints it) or "
        f"'https://shared.aito.ai/db/<name>' for a hosted database"
    )
