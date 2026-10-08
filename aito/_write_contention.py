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
- a capped number of retries, set per client.

Standard library only.
"""

import random
from typing import Any, Optional

#: the error code: v1 carries it in the body's ``error``, v2 in ``data.code``
WRITE_CONTENTION = 'write.contention'
#: full jitter on top of Retry-After: up to BASE * 2**n seconds, at most CAP
BACKOFF_BASE = 0.5
BACKOFF_CAP = 8.0
#: the clients' default number of retries
DEFAULT_RETRIES = 3


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
    """the Retry-After header in seconds; 1 when absent or not a number (an HTTP date included)"""
    raw = None
    for name, value in (headers or {}).items():
        if str(name).lower() == 'retry-after':
            raw = value
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return 1.0


def delay(headers: Any, retry: int) -> float:
    """seconds to wait before retry number ``retry`` (0-based)"""
    return retry_after(headers) + random.uniform(0.0, min(BACKOFF_CAP, BACKOFF_BASE * 2 ** retry))


def check_retries(retries: int) -> int:
    """a client's write_contention_retries setting, validated"""
    if retries < 0:
        raise ValueError(f'write_contention_retries must be 0 or more, got {retries}')
    return retries
