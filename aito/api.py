"""Removed in aitoai 1.0: the v1 helper functions' old home.

Importing it raises an ``ImportError`` that names the replacement.
"""

raise ImportError(
    "`aito.api` was removed in aitoai 1.0. Use `aito.v1.api` instead "
    "(e.g. `import aito.v1.api as aito_api`), or pin `aitoai<1`."
)
