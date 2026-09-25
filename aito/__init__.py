"""Aito.ai Python SDK

Each Aito API version has its own package, whose meaning never changes:

    from aito.v1 import Client      # the v1 API
    from aito.v2 import Client      # the v2 API

``aito.Client`` is the one name that follows the current default version. It moves
only on a MAJOR release of this package, so the major version says which API it is:
aitoai 0.x -> v1, 1.x -> v2. Quickstarts can use ``aito.Client``; production code
should import the explicit version. See ``docs/versioned-namespaces.md``.

``Client`` is resolved lazily. Importing a submodule runs this file first, so an
eager ``from aito.v1 import Client`` here would make ``import aito.v2`` load the whole
v1 stack — the import-weight defect fixed in 0.6.2, reintroduced one level up.
"""

__version__ = "0.7.0"

#: The API version ``aito.Client`` points at. Changes only on a major release.
DEFAULT_API_VERSION = 'v1'


def __getattr__(name):
    if name == 'Client':
        from importlib import import_module
        return import_module(f'aito.{DEFAULT_API_VERSION}').Client
    raise AttributeError(f"module 'aito' has no attribute {name!r}")


def __dir__():
    return sorted(list(globals()) + ['Client'])
