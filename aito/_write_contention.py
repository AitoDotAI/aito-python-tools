"""The retry policy for a starved write: 409 ``write.contention`` (aito-core #1619)

A large write that loses to concurrent writes on the same table gives up without
committing anything and answers 409 with the code ``write.contention`` and a
``Retry-After`` header. Nothing was written, so the request is safe to repeat; the v1 and
v2 clients both do, by this one policy:

- keyed on the CODE, never on 409 alone: the server's other 409s (an env being migrated, an
  old binary format that needs a repair, a release or branch conflict) are other conditions;
- the server's Retry-After (seconds; 1 when absent or not a number) is the floor, plus full
  jitter up to ``BASE * 2**n`` seconds (at most ``CAP``), so clients starved by the same
  churn do not all retry on the same beat;
- a capped number of retries, set per client;
- never for a job's status or result (``GET .../jobs/{id}`` and ``.../result``): a stored
  result does not change, so the retry that helps is submitting the job again.

A 429 (the server's admission control turning a request away for now) is retried the
same way, with its own count and a cap on the total wait: a Retry-After longer than the
time left is raised at once rather than slept through.

Standard library only.
"""

import random
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Optional

#: the error code: v1 carries it in the body's ``error``, v2 in ``data.code``
WRITE_CONTENTION = 'write.contention'
#: full jitter on top of Retry-After: up to BASE * 2**n seconds, at most CAP
BACKOFF_BASE = 0.5
BACKOFF_CAP = 8.0
#: the clients' default number of retries
DEFAULT_RETRIES = 3
#: what an error raised for a job's write.contention result adds: the job has to be resubmitted
JOB_CONTENTION_HINT = ('the job lost to concurrent writes on the same table and its result is '
                       'stored as is: pause the writers and submit the job again')

#: the status a busy server turns a request away with, asking to come back after Retry-After
RATE_LIMITED = 429
#: the clients' default number of 429 retries
DEFAULT_RATE_LIMIT_RETRIES = 3
#: the most a client waits in all for one request turned away with 429
MAX_RATE_LIMIT_WAIT = 60.0

_JOB_POLL = re.compile(r'/jobs/[^/?]+(/result)?/?(\?.*)?$')


def is_job_poll(method: str, path: str) -> bool:
    """a GET of a job's status or stored result, which repeating never changes"""
    return method.upper() == 'GET' and bool(_JOB_POLL.search(path))


def code_of(body: Any) -> Optional[str]:
    """the error code in a parsed error body, v1 (``error``) or v2 (``data.code``)"""
    if not isinstance(body, dict):
        return None
    data = body.get('data')
    if isinstance(data, dict) and isinstance(data.get('code'), str):
        return data['code']
    error = body.get('error')
    return error if isinstance(error, str) else None


def retry_after(headers: Any) -> float:
    """the Retry-After header in seconds, given as seconds or as an HTTP date; 1 when absent
    or unreadable"""
    raw = None
    for name, value in (headers or {}).items():
        if str(name).lower() == 'retry-after':
            raw = value
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        pass
    try:
        when = parsedate_to_datetime(str(raw))
    except (TypeError, ValueError, IndexError):
        return 1.0
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def delay(headers: Any, retry: int) -> float:
    """seconds to wait before retry number ``retry`` (0-based)"""
    return retry_after(headers) + random.uniform(0.0, min(BACKOFF_CAP, BACKOFF_BASE * 2 ** retry))


def check_retries(retries: int, name: str = 'write_contention_retries') -> int:
    """a client's retry count setting, validated"""
    if retries < 0:
        raise ValueError(f'{name} must be 0 or more, got {retries}')
    return retries
