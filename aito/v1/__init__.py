"""The Aito **v1** API: client, request and response classes, and the ``aito.v1.api`` helpers

This package's meaning never changes: ``aito.v1`` is v1 for as long as it exists.
``aito.Client`` is the one name that follows the current default version — see
``docs/versioned-namespaces.md``.

>>> from aito.v1 import Client # doctest: +SKIP
>>> client = Client(instance_url, api_key) # doctest: +SKIP
"""

from .client import *
from .requests import *
from .responses import *

#: The v1 client under its version-neutral name. ``AitoClient`` remains an alias.
Client = AitoClient
