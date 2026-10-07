# Aito plugin for Claude Code

A skill, `add-predictive-feature`, and the `aito` MCP server, so that a coding agent
adds a predictive feature with Aito where Aito fits, and says so when it does not.

- **The skill** walks the agent through an honest fit check (with the cases where a
  language model, a trained model or a search engine is the better tool), the data
  separation decision for multi-tenant apps, a schema derived from the user's
  existing table, a first `_predict`, a threshold chosen on the user's own held-out
  rows with `_evaluate`, and act / assist / abstain wiring.
- **The MCP server** (`aito-mcp`, from `pip install "aitoai[mcp]"`) gives the agent
  predict, recommend, relate, match, search, query, evaluate and schema tools, each
  describing when to use it and when not. Writes are off unless the user starts it
  with `AITO_MCP_ALLOW_WRITES=1`.

The server finds the instance the way the Python SDK does: `AITO_URL` and
`AITO_API_KEY`, or the profile that `aito start` writes for a local instance.

## Try it locally

```
claude --plugin-dir ./claude-plugin
```

`.mcp.json` runs the server with `uvx --from "aitoai[mcp]" aito-mcp`, which needs
an aitoai release that has the `mcp` extra (Python 3.10+).

The fit and non-fit lists follow the page "When to use Aito, and when not",
https://aito.ai/docs/articles/when-to-use-aito-and-when-not/; the evidence behind each line is at
https://aito.ai/docs/api/v2/benchmarks/
