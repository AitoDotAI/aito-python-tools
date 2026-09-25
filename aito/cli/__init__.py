"""The ``aito`` command-line tool

Needs the ``aitoai[cli]`` extra. :func:`main` is the console entry point: it imports the
real parser lazily, so a bare ``pip install aitoai`` gets a one-line instruction instead
of a ``ModuleNotFoundError`` traceback.
"""

import sys


def main():
    from aito.utils._optional import CLI_EXTRA, INSTALL_HINT
    try:
        from aito.cli.main_parser import main as _main
    except ImportError as e:
        missing = (getattr(e, 'name', None) or '').split('.')[0]
        if missing in CLI_EXTRA or isinstance(e.__cause__, ImportError):
            sys.exit(f"aito: the command-line tool needs extra dependencies "
                     f"({missing or 'see above'} is not installed). Install them with:\n"
                     f"  {INSTALL_HINT}")
        raise
    _main()
