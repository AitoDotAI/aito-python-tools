"""Small things a first-time user meets: the key prompt, a warning, an unconfigured MCP server"""

import asyncio
import subprocess
import sys
import unittest
from unittest import mock

from tests.cases import BaseTestCase

try:
    from aito.mcp import server as mcp_server
except ImportError:
    mcp_server = None


class TestConfigureHidesTheKey(BaseTestCase):
    def test_the_api_key_is_read_without_echo(self):
        from aito.cli.sub_commands import database_sub_command as cmd
        with mock.patch.object(cmd, 'get_existing_credentials', return_value=(None, None)), \
                mock.patch('builtins.input', return_value='https://example.aito.app') as typed, \
                mock.patch.object(cmd, 'getpass', return_value='secret-key', create=True) as hidden, \
                mock.patch.object(cmd, 'AitoClient'), \
                mock.patch.object(cmd, 'write_credentials_file_profile') as write:
            cmd.ConfigureSubCommand().parse_and_execute({'profile': 'p'})
        self.assertEqual(typed.call_count, 1)            # the URL is typed visibly
        self.assertEqual(hidden.call_count, 1)           # the key is not
        write.assert_called_once_with('p', 'https://example.aito.app', 'secret-key')


class TestNoInvalidEscapeWarning(BaseTestCase):
    def test_the_credentials_module_compiles_without_a_syntax_warning(self):
        res = subprocess.run([sys.executable, '-W', 'error::SyntaxWarning', '-W', 'error::DeprecationWarning', '-c',
                              'import importlib, aito.utils._credentials_file_utils as m; '
                              'compile(open(m.__file__).read(), m.__file__, "exec")'],
                             capture_output=True, text=True)
        self.assertEqual(res.returncode, 0, res.stderr)


@unittest.skipIf(mcp_server is None, 'the mcp extra is not installed')
class TestAnUnconfiguredServerExplainsTheWaysIn(BaseTestCase):
    """with no instance, the server still starts, so the agent can read why and tell the user"""

    def test_every_tool_answers_with_the_setup_steps(self):
        server = mcp_server.build_server(None, allow_writes=False)
        tools = [t.name for t in asyncio.run(server.list_tools())]
        self.assertIn('predict', tools)
        with self.assertRaises(mcp_server.ToolError) as ctx:
            asyncio.run(server.call_tool('predict', {'query': {'from': 't', 'predict': 'x'}}))
        message = str(ctx.exception)
        for way in ('aito start', 'AITO_URL', 'aito.ai'):
            self.assertIn(way, message)
        self.assertNotIn('sandbox', message.lower())    # no automatic shared sandbox (CRO, 10.10)
        self.assertIn('NOT CONFIGURED', server.instructions)

    def test_main_starts_it_instead_of_exiting(self):
        from aito.local.profiles import NoCredentialsError
        with mock.patch('aito.v2.client.resolve_credentials', side_effect=NoCredentialsError('none')), \
                mock.patch.object(mcp_server, 'build_server') as build:
            mcp_server.main()
        build.assert_called_once()
        self.assertIsNone(build.call_args[0][0])

    def test_the_upload_tool_states_the_free_mode_cap(self):
        server = mcp_server.build_server(None, allow_writes=False)
        upload = next(t for t in asyncio.run(server.list_tools()) if t.name == 'upload_rows')
        self.assertIn('10,000 rows per table', upload.description)
