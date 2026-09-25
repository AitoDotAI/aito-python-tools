"""DEPRECATED: the v1 helper functions' old home. Use ``aito.v1.api``.

This module replaces itself with ``aito.v1.api`` in ``sys.modules``, so ``aito.api`` and
``aito.v1.api`` are the SAME module object — patching or monkeypatching either one
affects both, exactly as before the move. Removed in aitoai 1.0.
"""

import sys
import warnings

warnings.warn(
    "`aito.api` is deprecated and will be removed in aitoai 1.0. "
    "Use `aito.v1.api` instead (e.g. `import aito.v1.api as aito_api`).",
    DeprecationWarning, stacklevel=2,
)

from aito.v1 import api as _api  # noqa: E402

sys.modules[__name__] = _api
