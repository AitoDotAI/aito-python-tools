# aito-mcp

An MCP server for [Aito](https://aito.ai), the predictive database, for AI assistants and
coding agents: `uvx aito-mcp`.

Its tools are predict, recommend, relate, match, search, query, evaluate, get_schema,
put_schema and upload_rows over the Aito v2 API. Each tool's description says when Aito
fits and when another tool (a language model, a trained model, a search engine) is the
better choice. Results carry `$p` (a probability you can set a threshold on) and `$why`
(the evidence). It is read-only unless started with `AITO_MCP_ALLOW_WRITES=1`.

The server finds the instance like the Aito Python SDK: `AITO_URL` and `AITO_API_KEY`, or
the profile `aito start` writes for a local Aito (`pip install aitoai`, then `aito start`).

    claude mcp add aito -- uvx aito-mcp

The code lives in [aitoai](https://pypi.org/project/aitoai/) (`aito.mcp`); this package
only installs it with the `mcp` extra. Source: https://github.com/AitoDotAI/aito-python-tools

<!-- mcp-name: io.github.AitoDotAI/aito -->
