"""Errors raised by the Aito v2 client

An error from the **query** path comes back as the structured `error` kind::

    {"kind": "error", "data": {"code": "not_found", "message": "x not found"}}

:class:`AitoV2Error` parses that into a machine-readable :attr:`~AitoV2Error.code`
so callers branch on the code instead of matching substrings of the message.

Not every v2 error is that shape yet, though the spec says it should be. The
schema and data endpoints fall through to the handler they share with v1 and
answer a missing table with the flat v1 body instead::

    GET /api/v2/schema/no_such_thing
    → 404 {"message": "No such table: no_such_thing", "status": 404}

There is no `code` in that, so :attr:`~AitoV2Error.code` is ``None`` on exactly
the path a drop-if-exists loader takes. :attr:`~AitoV2Error.is_not_found` falls
back to the HTTP status for that reason — use it rather than comparing the code
yourself. (Filed against the engine; when it emits the structured shape
everywhere, the fallback becomes redundant rather than wrong.)
"""

import logging
from typing import Any, Optional

from aito.exceptions import BaseError

LOG = logging.getLogger('AitoClientV2')


class AitoV2Error(BaseError):
    """An error occurred when calling the Aito v2 API

    Carries the HTTP status, the machine-readable error code and the raw body so
    a caller can diagnose without a debugger.

    >>> try: # doctest: +SKIP
    ...     client.predict(from_table='invoices', where={}, predict='gl_code')
    ... except AitoV2Error as err:
    ...     if err.code == 'not_found':
    ...         ...  # the collection does not exist yet
    """

    #: the error codes the engine emits from the query path
    QUERY_CODES = frozenset({
        'query.invalid', 'request.invalid', 'proposition.unsupported',
        'operation.unsupported', 'not_found', 'json.malformed', 'internal',
    })

    #: the error codes the engine emits from the schema and data endpoints
    SCHEMA_DATA_CODES = frozenset({
        'data.bad_request', 'import.failed', 'table.exists', 'schema.not_collection',
        'backfill.failed', 'schema.create_failed', 'schema.column_update_failed',
        'schema.refresh_failed', 'schema.plan_failed', 'schema.apply_failed',
    })

    def __init__(
            self,
            message: str,
            code: Optional[str] = None,
            status_code: Optional[int] = None,
            body: Any = None
    ):
        """

        :param message: the human-readable error message
        :type message: str
        :param code: the machine-readable error code, if the response carried one
        :type code: Optional[str]
        :param status_code: the HTTP status code of the response, if there was a response
        :type status_code: Optional[int]
        :param body: the raw response body
        :type body: Any
        """
        self.code = code
        self.status_code = status_code
        self.body = body
        self.message = message
        # Deliberately NOT BaseError.__init__, which logs at ERROR on
        # construction. An exception that is about to be raised is already
        # visible to the caller, and plenty of them are expected: the
        # drop-if-exists idiom raises a 404 on every first run, and logging that
        # at ERROR trains people to ignore the error log. Raising is the report;
        # this is a breadcrumb for when it is caught and swallowed.
        Exception.__init__(self, message)
        LOG.debug('%s: %s', self.__class__.__name__, message)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.message})'

    @property
    def is_not_found(self) -> bool:
        """whether the error is a missing table, collection or environment

        Replaces the substring matching (``"failed to open '<table>'"``) that
        callers needed before the engine emitted structured codes.

        :rtype: bool
        """
        return self.code == 'not_found' or self.status_code == 404

    @classmethod
    def from_response(cls, status_code: int, body: str, parsed: Any = None) -> 'AitoV2Error':
        """build an error from a failed v2 response

        Falls back to the raw body when the response is not the structured error
        shape — an error from a proxy or a framework-level auth failure is not
        guaranteed to be one.

        :param status_code: the HTTP status code of the response
        :type status_code: int
        :param body: the raw response text
        :type body: str
        :param parsed: the parsed JSON body, if it could be parsed
        :type parsed: Any
        :rtype: AitoV2Error
        """
        code = None
        message = None
        if isinstance(parsed, dict):
            data = parsed.get('data')
            if parsed.get('kind') == 'error' and isinstance(data, dict):
                code = data.get('code')
                message = data.get('message')
            elif 'message' in parsed:
                # framework-level errors (auth, no route) keep the shape shared with v1
                message = parsed.get('message')
        if not message:
            message = body[:500] if body else f'HTTP {status_code}'
        prefix = f'Aito v2 returned {status_code}'
        if code:
            prefix += f' [{code}]'
        return cls(f'{prefix}: {message}', code=code, status_code=status_code, body=body)


class AitoV2ResponseError(AitoV2Error):
    """The response was not the shape the requested operation produces

    Raised when the envelope carries a ``kind`` other than the one the called
    endpoint returns — the query did something other than what the caller thinks.
    """
