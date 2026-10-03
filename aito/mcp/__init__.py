"""An MCP server over the Aito v2 API: ``pip install aitoai[mcp]``, then run ``aito-mcp``

See :mod:`aito.mcp.server`. The ``aito-mcp`` command is installed with every aitoai,
the ``mcp`` SDK it needs only with the extra, so the entry point imports it late.
"""


def main() -> None:
    """``aito-mcp``: serve the tools over stdio, or say which extra to install"""
    try:
        from aito.mcp.server import main as serve
    except ImportError as e:
        if getattr(e, 'name', None) is None or not e.name.startswith('mcp'):
            raise
        raise SystemExit("aito-mcp needs the mcp extra: pip install 'aitoai[mcp]' (Python 3.10+)") from e
    serve()
