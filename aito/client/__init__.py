"""Removed in aitoai 1.0: the pre-0.7 home of the clients.

Importing it raises an ``ImportError`` that names the replacement, rather than a bare
``ModuleNotFoundError``: an upgrade to 1.0 should tell the caller what to write.
``aito.client.v2`` and ``aito.client.requests`` etc. land here too, since a subpackage
import runs this file first.
"""

raise ImportError(
    "`aito.client` was removed in aitoai 1.0. "
    "For the v1 API: `from aito.v1 import Client` (was `aito.client.AitoClient`). "
    "For the v2 API: `from aito.v2 import Client` (was `aito.client.v2.AitoClientV2`). "
    "Requests and responses: `aito.v1.requests` / `aito.v1.responses`. "
    "Or pin `aitoai<1` to keep the old paths."
)
