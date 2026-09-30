"""Credential resolution for ``aito.Client()`` and the profile store ``aito start`` writes (offline)"""

import os
import stat
import unittest
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import mock

from aito.local import profiles
from aito.v2 import AitoClientV2
from tests.cases import BaseTestCase

LOCAL = {'instance_url': 'http://127.0.0.1:9005', 'api_key': 'rw-local', 'read_only_api_key': 'ro-local'}
CLOUD = {'instance_url': 'https://shared.aito.ai/db/x', 'api_key': 'rw-cloud'}


class TestResolveCredentials(BaseTestCase):
    def setUp(self):
        super().setUp()
        self._dir = tempfile.TemporaryDirectory()
        base = Path(self._dir.name) / 'aito'
        self._patches = [
            mock.patch.object(profiles, 'CREDENTIALS_FILE', base / 'credentials'),
            mock.patch.object(profiles, 'SETTINGS_FILE', base / 'config'),
            mock.patch.dict(os.environ, {}, clear=False),
        ]
        for p in self._patches:
            p.start()
        for var in profiles.URL_ENV_VARS + (profiles.KEY_ENV_VAR, profiles.PROFILE_ENV_VAR):
            os.environ.pop(var, None)

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()
        self._dir.cleanup()
        super().tearDown()

    def test_explicit_arguments_win(self):
        profiles.save_profile('local', LOCAL)
        profiles.set_active_profile('local')
        os.environ.update(AITO_URL='http://env', AITO_API_KEY='env-key')
        self.assertEqual(profiles.resolve_credentials('https://arg', 'arg-key'), ('https://arg', 'arg-key'))

    def test_environment_beats_the_profile(self):
        profiles.save_profile('local', LOCAL)
        profiles.set_active_profile('local')
        os.environ.update(AITO_URL='http://env', AITO_API_KEY='env-key')
        self.assertEqual(profiles.resolve_credentials(), ('http://env', 'env-key'))

    def test_the_older_env_name_still_works(self):
        os.environ.update(AITO_INSTANCE_URL='http://old', AITO_API_KEY='k')
        self.assertEqual(profiles.resolve_credentials(), ('http://old', 'k'))

    def test_the_active_profile_is_the_fallback(self):
        profiles.save_profile('local', LOCAL)
        profiles.set_active_profile('local')
        self.assertEqual(profiles.resolve_credentials(), ('http://127.0.0.1:9005', 'rw-local'))

    def test_aito_profile_env_selects_another_profile(self):
        profiles.save_profile('local', LOCAL)
        profiles.save_profile('cloud', CLOUD)
        profiles.set_active_profile('local')
        os.environ['AITO_PROFILE'] = 'cloud'
        self.assertEqual(profiles.resolve_credentials(), (CLOUD['instance_url'], 'rw-cloud'))

    def test_a_url_alone_takes_the_key_stored_for_that_url(self):
        profiles.save_profile('local', LOCAL)
        profiles.save_profile('cloud', CLOUD)
        profiles.set_active_profile('local')
        self.assertEqual(profiles.resolve_credentials(CLOUD['instance_url'] + '/'),
                         (CLOUD['instance_url'] + '/', 'rw-cloud'))

    def test_localhost_and_127_0_0_1_name_the_same_instance(self):
        # the profile stores 127.0.0.1 (IPv4-only publish); a user typing localhost still gets its key
        profiles.save_profile('local', LOCAL)
        self.assertEqual(profiles.resolve_credentials('http://localhost:9005'),
                         ('http://localhost:9005', 'rw-local'))

    def test_a_url_with_no_stored_key_fails(self):
        profiles.save_profile('local', LOCAL)
        profiles.set_active_profile('local')
        with self.assertRaises(profiles.NoCredentialsError) as ctx:
            profiles.resolve_credentials('https://elsewhere')
        self.assertIn('no API key for https://elsewhere', str(ctx.exception))

    def test_a_key_alone_is_never_sent_to_the_profiles_instance(self):
        profiles.save_profile('local', LOCAL)
        profiles.set_active_profile('local')
        with self.assertRaises(profiles.NoCredentialsError) as ctx:
            profiles.resolve_credentials(api_key='some-other-key')
        self.assertIn('no instance URL', str(ctx.exception))

    def test_nothing_configured_names_every_way_out(self):
        with self.assertRaises(profiles.NoCredentialsError) as ctx:
            profiles.resolve_credentials()
        message = str(ctx.exception)
        for hint in ('Client(instance_url, api_key)', 'AITO_URL', 'aito start'):
            self.assertIn(hint, message)

    @unittest.skipIf(os.name == 'nt', 'POSIX file modes: on Windows the per-user profile directory is private by ACL')
    def test_the_store_is_owner_only(self):
        profiles.save_profile('local', LOCAL)
        profiles.set_active_profile('local')
        for path in (profiles.CREDENTIALS_FILE, profiles.SETTINGS_FILE):
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600, path)
        self.assertEqual(stat.S_IMODE(profiles.CREDENTIALS_FILE.parent.stat().st_mode), 0o700)

    def test_saving_one_profile_keeps_the_others(self):
        profiles.save_profile('default', CLOUD)
        profiles.save_profile('local', LOCAL)
        profiles.save_profile('local', {'image': 'x'})
        self.assertEqual(profiles.load_profile('default'), CLOUD)
        self.assertEqual(profiles.load_profile('local'), {**LOCAL, 'image': 'x'})

    def test_client_with_no_arguments_uses_the_active_profile(self):
        profiles.save_profile('local', LOCAL)
        profiles.set_active_profile('local')
        client = AitoClientV2(check_credentials=False)
        self.assertEqual((client.instance_url, client.api_key), ('http://127.0.0.1:9005', 'rw-local'))


class TestLoopbackIsLiteral(BaseTestCase):
    """macOS resolves localhost to ::1 first, and the ports are published on 127.0.0.1 only"""

    def test_the_url_aito_start_stores_is_127_0_0_1(self):
        from aito.local.server import ServerConfig
        self.assertEqual(ServerConfig().url, 'http://127.0.0.1:9005')
        self.assertEqual(ServerConfig(port=19005).url, 'http://127.0.0.1:19005')

    def test_every_printed_line_uses_127_0_0_1(self):
        from aito.local.cli import _connection_block
        from aito.local.server import ServerConfig
        block = _connection_block(ServerConfig(sql_port=5433), {'api_key': 'rw', 'read_only_api_key': 'ro'}, True)
        self.assertNotIn('localhost', block)
        self.assertIn('AITO_URL=http://127.0.0.1:9005', block)
        self.assertIn('psql -h 127.0.0.1 -p 5433', block)

    def test_the_sdk_uses_the_stored_url_unchanged(self):
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(profiles, 'CREDENTIALS_FILE', Path(d) / 'credentials'), \
                mock.patch.object(profiles, 'SETTINGS_FILE', Path(d) / 'config'), \
                mock.patch.dict(os.environ, {'AITO_PROFILE': 'local'}):
            for var in profiles.URL_ENV_VARS + (profiles.KEY_ENV_VAR,):
                os.environ.pop(var, None)
            profiles.save_profile('local', LOCAL)
            client = AitoClientV2(check_credentials=False)
            self.assertEqual(client.api_url, 'http://127.0.0.1:9005/api/v2')


class TestConfigureWritesOwnerOnly(BaseTestCase):
    @unittest.skipIf(os.name == 'nt', 'POSIX file modes: on Windows the per-user profile directory is private by ACL')
    def test_aito_configure_file_is_0600(self):
        from aito.utils._credentials_file_utils import write_credentials_file_profile
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'aito' / 'credentials'
            write_credentials_file_profile('default', 'https://x', 'k', credentials_file_path=path)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            write_credentials_file_profile('other', 'https://y', 'k2', credentials_file_path=path)
            self.assertEqual(sorted(profiles._read(path).sections()), ['default', 'other'])


class TestLocalCommandsNeedNoExtra(BaseTestCase):
    def _help(self, *argv):
        # the [cli] extra's modules made unimportable, as on a bare `pip install aitoai`
        code = ("import sys; sys.modules['pandas'] = None; sys.modules['argcomplete'] = None; "
                f"sys.argv = ['aito', {', '.join(repr(a) for a in argv)}]; from aito.cli import main; main()")
        return subprocess.run([sys.executable, '-c', code], capture_output=True, text=True)

    def test_start_help_runs_with_the_cli_extra_unimportable(self):
        res = self._help('start', '-h')
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn('usage: aito start', res.stdout)
        self.assertIn('--sql-port', res.stdout)

    def test_serve_run_and_up_hint_at_start_and_fail(self):
        # reserved for a possible foreground mode: never a silent alias of the background start
        for word in ('serve', 'run', 'up'):
            res = self._help(word)
            self.assertNotEqual(res.returncode, 0, word)
            self.assertIn('`aito start` starts a local Aito in the background', res.stderr, word)
            self.assertIn('a foreground mode may come later', res.stderr, word)
            self.assertEqual(res.stdout, '', word)

    def test_the_reserved_words_are_not_listed(self):
        import argparse
        from aito.local.cli import build_parser
        parser = build_parser()
        commands = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction)).choices
        self.assertIn('start', commands)
        for word in ('serve', 'run', 'up'):
            self.assertNotIn(word, commands, word)
