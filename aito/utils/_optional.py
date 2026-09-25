"""Import a dependency that ships only with the ``aitoai[cli]`` extra

Since 0.7.0 a bare ``pip install aitoai`` installs what the API clients need —
requests, jsonschema, aiohttp, ndjson — and not the dataframe and file-format
toolchain (pandas, fastparquet, openpyxl, xlrd, langdetect, argcomplete). The features
that genuinely need those (the CLI, schema inference, file conversion) import them
through here, so a missing one fails with the command that fixes it rather than a bare
``ModuleNotFoundError``.
"""

import importlib

#: The packages that come with ``aitoai[cli]``, by import name.
CLI_EXTRA = frozenset({'pandas', 'numpy', 'fastparquet', 'openpyxl', 'xlrd', 'langdetect', 'argcomplete'})

INSTALL_HINT = "pip install 'aitoai[cli]'"


def import_optional(name: str, feature: str = 'this feature'):
    """import `name`, or raise an ImportError that says how to install it"""
    try:
        return importlib.import_module(name)
    except ImportError as e:
        raise ImportError(
            f"{feature} needs `{name}`, which is not installed. "
            f"It ships with the command-line extra: {INSTALL_HINT}"
        ) from e
