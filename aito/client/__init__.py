"""DEPRECATED: the v1 client's old home. Use ``aito.v1`` (or ``aito.Client``).

``aito.client`` is FROZEN to v1. It does not follow the default API version: every
existing caller that writes ``from aito.client import AitoClient`` means v1, and
switching it to v2 underneath them would break them silently. The name that follows
the default is ``aito.Client``. See ``docs/versioned-namespaces.md``.

The old dotted module paths (``aito.client.requests.query_api_request`` and so on)
keep resolving, to the same module objects as their ``aito.v1`` counterparts.
Removed in aitoai 1.0.
"""

import importlib as _importlib
import pkgutil as _pkgutil
import sys as _sys
import warnings as _warnings

_warnings.warn(
    "`aito.client` is deprecated and will be removed in aitoai 1.0. "
    "Import the v1 client from `aito.v1` (e.g. `from aito.v1 import Client`), "
    "or use `aito.Client` for the current default API version.",
    DeprecationWarning, stacklevel=2,
)

import aito.v1 as _v1  # noqa: E402
from aito.v1 import *  # noqa: E402,F401,F403


def _alias(old, module):
    _sys.modules[old] = module
    parent, _, attr = old.rpartition('.')
    if parent in _sys.modules:
        setattr(_sys.modules[parent], attr, module)


# `aito.client.aito_client` was the v1 client module; it is `aito.v1.client` now.
_alias('aito.client.aito_client', _importlib.import_module('aito.v1.client'))
for _pkg in ('requests', 'responses'):
    _mod = _importlib.import_module(f'aito.v1.{_pkg}')
    _alias(f'aito.client.{_pkg}', _mod)
    for _info in _pkgutil.iter_modules(_mod.__path__):
        _alias(f'aito.client.{_pkg}.{_info.name}',
               _importlib.import_module(f'aito.v1.{_pkg}.{_info.name}'))
