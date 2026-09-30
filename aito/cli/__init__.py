"""The ``aito`` command-line tool

Needs the ``aitoai[cli]`` extra. :func:`main` is the console entry point: it imports the
real parser lazily, so a bare ``pip install aitoai`` gets a one-line instruction instead
of a ``ModuleNotFoundError`` traceback.
"""

import sys


def main():
    # The local server commands need only the standard library, so they are dispatched
    # before the [cli] extra is imported: `pip install aitoai && aito start` works.
    from aito.local.cli import ALIASES as LOCAL_ALIASES, COMMANDS as LOCAL_COMMANDS
    if len(sys.argv) > 1 and (sys.argv[1] in LOCAL_COMMANDS or sys.argv[1] in LOCAL_ALIASES):
        from aito.local.cli import main as local_main
        sys.exit(local_main(sys.argv[1:]))

    from aito.utils._optional import CLI_EXTRA, INSTALL_HINT
    try:
        from aito.cli.main_parser import main as _main
    except ImportError as e:
        missing = (getattr(e, 'name', None) or '').split('.')[0]
        if missing in CLI_EXTRA or isinstance(e.__cause__, ImportError):
            sys.exit(f"aito: the command-line tool needs extra dependencies "
                     f"({missing or 'see above'} is not installed). Install them with:\n"
                     f"  {INSTALL_HINT}\n"
                     f"(`aito start`, `stop`, `status`, `logs`, `keys` and `upgrade` work without them.)")
        raise
    _main()
