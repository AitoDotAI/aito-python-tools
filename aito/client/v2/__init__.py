"""DEPRECATED: the v2 client's old home. Use ``aito.v2``.

The old dotted paths (``aito.client.v2.client`` and so on) keep resolving, to the same
module objects as ``aito.v2``. Importing this path also imports the v1 stack, because
``aito.client`` is its parent package — one more reason to move to ``aito.v2``, which
does not. Removed in aitoai 1.0.
"""

import importlib as _importlib
import sys as _sys
import warnings as _warnings

_warnings.warn(
    "`aito.client.v2` is deprecated and will be removed in aitoai 1.0. "
    "Import from `aito.v2` instead (e.g. `from aito.v2 import Client`).",
    DeprecationWarning, stacklevel=2,
)

from aito.v2 import *  # noqa: E402,F401,F403
from aito.v2 import __all__  # noqa: E402,F401

for _name in ('client', 'errors', 'responses'):
    _mod = _importlib.import_module(f'aito.v2.{_name}')
    _sys.modules[f'aito.client.v2.{_name}'] = _mod
    setattr(_sys.modules[__name__], _name, _mod)
