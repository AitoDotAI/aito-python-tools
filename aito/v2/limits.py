"""The engine's request-body limit, and how an upload stays under it

``POST /api/v2/data/{t}/batch`` and ``/data/{t}/import`` refuse a body over 10,000,000 bytes
with 413 ``payload_too_large``. Measured on v2.12.0 on 2026-11-13: a body of exactly
10,000,000 bytes is accepted, and 10,000,001 is refused. Through a proxy, the client may see a
reset or a broken pipe instead of the 413. The engine's limits page says "Max request body:
10 MB". Its v2 page says /batch and /import aren't capped, which contradicts the measurement;
board ticket td-20261113082157454471 fixes the page.

The client aims each request at ``TARGET_REQUEST_BYTES``, 10% under the limit, so a JSON
encoder that writes a few bytes more than ``json.dumps`` (simplejson, say) can't push a
batch over.
"""

import json
from typing import Any, List, Sequence, Tuple

#: the engine refuses a request body larger than this (measured on v2.12.0, see above)
MAX_REQUEST_BYTES = 10_000_000

#: the size the client aims each upload request at
TARGET_REQUEST_BYTES = 9_000_000


def json_bytes(value: Any) -> int:
    """the size of ``value`` as ``requests`` sends it with ``json=``"""
    # allow_nan: a NaN is refused by requests itself, after this, as before
    return len(json.dumps(value).encode('utf-8'))


def batch_body_bytes(row_sizes: Sequence[int]) -> int:
    """the size of a JSON array of rows of these sizes

    The brackets, and a comma and a space between rows, as ``json.dumps`` writes them.
    """
    return 2 + sum(row_sizes) + 2 * max(len(row_sizes) - 1, 0)


def over_the_limit_message(nbytes: int) -> str:
    return (f'the request body is {nbytes:,} bytes, over the engine\'s limit of '
            f'{MAX_REQUEST_BYTES:,} bytes')


def plan_batches(entries: List[Any], batch_size: int) -> List[Tuple[int, int]]:
    """``(start, end)`` ranges of at most ``batch_size`` rows and ``TARGET_REQUEST_BYTES``

    A row too big to share a request goes alone, as long as it fits the limit.

    :raises OverTheLimit: a row is over the limit by itself (checked before any range is returned)
    """
    sizes = [json_bytes(row) for row in entries]
    for i, size in enumerate(sizes):
        if batch_body_bytes([size]) > MAX_REQUEST_BYTES:
            raise OverTheLimit(i, batch_body_bytes([size]))
    ranges = []
    start, body = 0, 2
    for i, size in enumerate(sizes):
        grown = body + size + (2 if i > start else 0)
        if i > start and (i - start >= batch_size or grown > TARGET_REQUEST_BYTES):
            ranges.append((start, i))
            start, grown = i, 2 + size
        body = grown
    if start < len(entries):
        ranges.append((start, len(entries)))
    return ranges


class OverTheLimit(Exception):
    """a single row is bigger than a request may be"""

    def __init__(self, row: int, nbytes: int):
        self.row, self.nbytes = row, nbytes
        super().__init__(f'row {row} is {nbytes:,} bytes as a batch, over the engine\'s request-body '
                         f'limit of {MAX_REQUEST_BYTES:,} bytes, so it can never be uploaded; '
                         f'nothing was sent')
