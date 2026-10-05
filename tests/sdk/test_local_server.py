"""`aito start` and friends against a fake Docker (offline): the start/profile logic

The Docker path itself runs in CI (.github/workflows/local-server.yml); these pin the
decisions around it: what a rerun reuses, when the profile is written, whose container
may be replaced, and how an incomplete profile fails.
"""

import os
import tempfile
from pathlib import Path
from unittest import mock

from aito.local import profiles, server
from tests.cases import BaseTestCase


class FakeDocker:
    """just enough of `docker` for server.py: containers by name, and nothing else running"""

    def __init__(self):
        self.containers = {}
        self.runs = []

    def inspect(self, name):
        return self.containers.get(name)

    def run(self, cfg, keys):
        self.runs.append((cfg.container, cfg.image, cfg.port, cfg.sql_port, dict(keys)))
        self.containers[cfg.container] = {
            'State': {'Running': True},
            'Config': {'Labels': {'ai.aito.managed-by': 'aitoai-cli', server.PROFILE_LABEL: cfg.profile},
                       'Image': cfg.image,
                       'Env': [f"READ_WRITE_APIKEY={keys['api_key']}", f"APIKEY={keys['read_only_api_key']}"]},
            'Image': 'sha256:' + cfg.image[-8:],
            'HostConfig': {'PortBindings': {'9005/tcp': [{'HostPort': str(cfg.port)}],
                                            '5432/tcp': [{'HostPort': str(cfg.sql_port)}]}},
        }

    def remove(self, name):
        self.containers.pop(name, None)


class LocalServerCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self._dir = tempfile.TemporaryDirectory()
        base = Path(self._dir.name)
        self.docker = FakeDocker()
        d = self.docker
        patches = [
            mock.patch.object(profiles, 'CREDENTIALS_FILE', base / 'credentials'),
            mock.patch.object(profiles, 'SETTINGS_FILE', base / 'config'),
            mock.patch.dict(os.environ, {}),
            mock.patch.object(server, 'check_docker', lambda: None),
            mock.patch.object(server, '_inspect', d.inspect),
            mock.patch.object(server, '_run_container', d.run),
            mock.patch.object(server, '_docker', self._fake_docker_cmd),
            mock.patch.object(server, '_image_present', lambda image: True),
            mock.patch.object(server, '_image_id', lambda image: image[-8:]),
            mock.patch.object(server, '_volume_exists', lambda volume: False),
            mock.patch.object(server, '_port_free', lambda port: True),
            mock.patch.object(server, '_emulation_note', lambda image: None),
            mock.patch.object(server, 'wait_healthy', lambda *a, **k: 1.0),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(self._dir.cleanup)
        for var in profiles.URL_ENV_VARS + (profiles.KEY_ENV_VAR, profiles.PROFILE_ENV_VAR):
            os.environ.pop(var, None)

    def _fake_docker_cmd(self, *args, **kwargs):
        if args[:2] == ('rm', '-f'):
            self.docker.remove(args[2])
        return mock.Mock(returncode=0, stdout='', stderr='')

    def start(self, profile='local', **given):
        return server.start(server.ServerConfig.for_start(profile, **given), log=lambda m: None)


class TestRerunsKeepTheStoredServer(LocalServerCase):
    def test_a_plain_rerun_does_not_move_the_image(self):
        # after `aito start --image X`, a plain `aito start` must not silently go back to the pin
        self.start(image='ghcr.io/aitohq/aito:v2.99.0@sha256:0000newer')
        self.docker.containers['aito']['State']['Running'] = False      # e.g. after `aito stop`
        self.start()
        self.assertEqual(self.docker.runs[-1][1], 'ghcr.io/aitohq/aito:v2.99.0@sha256:0000newer')

    def test_a_plain_rerun_keeps_a_custom_port(self):
        self.start(port=9006)
        res = self.start()
        self.assertEqual(res['state'], 'already running')
        self.assertEqual(profiles.load_profile('local')['instance_url'], 'http://127.0.0.1:9006')

    def test_a_fallback_sql_port_does_not_defeat_already_running(self):
        with mock.patch.object(server, '_port_free', lambda port: port != 5432):
            first = self.start()
        self.assertEqual(first['config'].sql_port, 5433)
        second = self.start()
        self.assertEqual(second['state'], 'already running')
        self.assertEqual(len(self.docker.runs), 1, "the rerun recreated the container")

    def test_the_first_start_uses_the_pin_and_the_default_names(self):
        res = self.start()
        cfg = res['config']
        self.assertEqual((cfg.container, cfg.volume, cfg.image, cfg.port, cfg.sql_port),
                         ('aito', 'aito-state', server.PINNED_IMAGE, 9005, 5432))


class TestAFailedStartIsStillManageable(LocalServerCase):
    def test_the_profile_exists_when_the_wait_fails(self):
        def fail(*a, **k):
            raise server.LocalServerError('did not answer')
        with mock.patch.object(server, 'wait_healthy', fail):
            with self.assertRaises(server.LocalServerError):
                self.start()
        cfg = server.ServerConfig.from_profile('local')           # logs/status/stop can find it
        self.assertEqual(cfg.container, 'aito')
        self.assertEqual(server.stored_keys('local'), {k: v for k, v in self.docker.runs[0][4].items()})

    def test_a_failed_first_start_does_not_become_the_active_profile(self):
        with mock.patch.object(server, 'wait_healthy', mock.Mock(side_effect=server.LocalServerError('x'))):
            with self.assertRaises(server.LocalServerError):
                self.start()
        self.assertNotEqual(profiles.active_profile_name(), 'local')


class TestProfilesDoNotShareContainers(LocalServerCase):
    def test_another_profile_gets_its_own_container_and_volume(self):
        self.start()
        res = self.start('other')
        self.assertEqual((res['config'].container, res['config'].volume), ('aito-other', 'aito-other-state'))
        self.assertIn('aito', self.docker.containers)          # local's is untouched

    def test_a_container_owned_by_another_profile_is_not_replaced(self):
        self.start()
        with self.assertRaises(server.LocalServerError) as ctx:
            self.start('other', container='aito', port=9006)
        self.assertIn("belongs to profile 'local'", str(ctx.exception))


class TestStartRefuses(LocalServerCase):
    def test_a_configure_profile_is_never_overwritten(self):
        profiles.save_profile('default', {'instance_url': 'https://x.aito.app', 'api_key': 'cloud-key'})
        with self.assertRaises(server.LocalServerError) as ctx:
            self.start('default')
        self.assertIn('would overwrite', str(ctx.exception))
        self.assertEqual(profiles.load_profile('default')['api_key'], 'cloud-key')

    def test_a_bad_port_is_refused_not_ignored(self):
        for given in ({'port': 0}, {'sql_port': 70000}, {'container': ''}):
            with self.assertRaises(server.LocalServerError, msg=given):
                server.ServerConfig.for_start('local', **given)

    def test_stop_leaves_another_profiles_container_alone(self):
        self.start()
        with self.assertRaises(server.LocalServerError):
            server.stop(server.ServerConfig(profile='other', container='aito'))
        self.assertTrue(self.docker.containers['aito']['State']['Running'])


class TestPortsAcrossProfiles(LocalServerCase):
    """from the fresh-eyes rerun (org/dx/2026-09-30-aito-start-first-hour.md)"""

    def test_a_new_profile_does_not_default_onto_locals_port(self):
        self.assertEqual(server.ServerConfig.for_start('demo').port, 9006)
        self.assertEqual(server.ServerConfig.for_start('local').port, 9005)

    def test_a_new_profile_skips_ports_in_use(self):
        with mock.patch.object(server, '_port_free', lambda port: port not in (9006, 9007)):
            self.assertEqual(server.ServerConfig.for_start('demo').port, 9008)

    def test_the_port_hint_keeps_the_profile(self):
        # following `aito start --port N` without --profile restarted `local` on N instead
        with mock.patch.object(server, '_port_free', lambda port: False):
            with self.assertRaises(server.LocalServerError) as ctx:
                self.start('demo', port=9100)
        self.assertIn('`aito start --profile demo --port 9101`', str(ctx.exception))


class TestMessagesNameTheCause(LocalServerCase):
    def test_a_port_held_by_another_profile_says_so(self):
        self.start()
        with mock.patch.object(server, '_port_free', lambda port: port != 9005):
            with self.assertRaises(server.LocalServerError) as ctx:
                self.start('demo', port=9005)
        self.assertIn("by the local Aito of profile 'local'", str(ctx.exception))

    def test_the_printed_curl_works_on_its_own(self):
        from aito.local.cli import _connection_block
        block = _connection_block(server.ServerConfig(), {'api_key': 'rw', 'read_only_api_key': 'ro'}, True)
        self.assertIn('curl -H "x-api-key: rw" http://127.0.0.1:9005/api/v2/schema', block)

    def test_an_installed_but_broken_module_is_not_called_missing(self):
        import importlib
        import importlib.util
        from aito.utils._optional import import_optional
        # find_spec first: mock.patch itself resolves targets through import_module
        with mock.patch.object(importlib.util, 'find_spec', return_value=object()), \
                mock.patch.object(importlib, 'import_module', side_effect=ImportError('libstdc++.so.6: cannot open')):
            with self.assertRaises(ImportError) as ctx:
                import_optional('pandas', 'schema inference')
        self.assertIn('installed but failed to import', str(ctx.exception))

    def test_a_refused_connection_to_a_local_url_hints_at_aito_start(self):
        import requests
        from aito.v2 import AitoClientV2, AitoV2Error
        client = AitoClientV2('http://127.0.0.1:9005', 'k', check_credentials=False)
        with mock.patch.object(client._session, 'request', side_effect=requests.ConnectionError('refused')):
            with self.assertRaises(AitoV2Error) as ctx:
                client.get_schema()
        self.assertIn('`aito status`', str(ctx.exception))


class TestIncompleteProfiles(LocalServerCase):
    def test_a_partial_profile_fails_with_a_message_not_a_traceback(self):
        profiles.save_profile('local', {'instance_url': 'http://127.0.0.1:9005', 'container': 'aito'})
        with self.assertRaises(server.LocalServerError) as ctx:
            server.ServerConfig.from_profile('local')
        self.assertIn('missing', str(ctx.exception))
        with self.assertRaises(server.LocalServerError):
            server.stored_keys('local')

    def test_start_repairs_a_partial_profile(self):
        profiles.save_profile('local', {'instance_url': 'http://127.0.0.1:9005', 'container': 'aito'})
        self.start()
        self.assertEqual(server.ServerConfig.from_profile('local').volume, 'aito-state')

    def test_a_vanished_container_counts_as_stopped(self):
        self.assertIn('no longer exists', server._exited('never-there'))


class TestSharedLocation(BaseTestCase):
    def test_configure_and_start_use_one_credentials_file(self):
        from aito.utils import _credentials_file_utils
        self.assertEqual(_credentials_file_utils.DEFAULT_CREDENTIAL_FILE, profiles.CREDENTIALS_FILE)

    def test_configure_drops_a_read_only_key_that_no_longer_matches(self):
        from aito.utils._credentials_file_utils import write_credentials_file_profile
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'credentials'
            write_credentials_file_profile('local', 'http://127.0.0.1:9005', 'rw1', credentials_file_path=path)
            config = profiles._read(path)
            config.set('local', 'read_only_api_key', 'ro1')
            profiles._write_private(path, config)
            write_credentials_file_profile('local', 'https://x', 'rw2', credentials_file_path=path)
            self.assertNotIn('read_only_api_key', profiles._read(path)['local'])


class TestImageForTheHostsCpu(BaseTestCase):
    """a copy of the pinned image pulled for another CPU must not shadow the host's variant

    Found preparing 1.2.0 (5.10): this machine held an arm64 copy of the multi-arch
    v2.11.2 (pulled with --platform elsewhere), and `aito start` on the amd64 host ran it
    and failed with 'exec format error'. The image was present, so nothing was pulled.
    """
    IMAGE = 'ghcr.io/aitohq/aito:v2.11.2@sha256:abc'

    def ensure(self, host, local_arch, pull_ok=True, present=True):
        calls = []
        arch = {'now': local_arch}

        def docker(*args, check=True, capture=True):
            calls.append(args)
            if args[:2] == ('version', '--format'):
                return mock.Mock(returncode=0, stdout=host + '\n', stderr='')
            if args[:3] == ('image', 'inspect', '--format'):
                return mock.Mock(returncode=0, stdout=arch['now'] + '\n', stderr='')
            if args[:2] == ('pull', '--platform'):
                if pull_ok:
                    arch['now'] = host
                return mock.Mock(returncode=0 if pull_ok else 1, stdout='',
                                 stderr='' if pull_ok else 'no matching manifest')
            return mock.Mock(returncode=0, stdout='', stderr='')

        with mock.patch.object(server, '_docker', docker), \
                mock.patch.object(server, '_image_present', lambda image: present):
            server._ensure_image(self.IMAGE, log=lambda m: None)
        return calls

    def test_a_copy_for_another_cpu_is_replaced_by_the_hosts_variant(self):
        # Docker's classic store maps a digest to one image ("cannot overwrite digest"):
        # the stale reference goes first, then the host's variant is pulled
        calls = self.ensure(host='amd64', local_arch='arm64')
        self.assertEqual([c for c in calls if c[0] in ('pull', 'image') and c[1] in ('rm', '--platform')],
                         [('image', 'rm', self.IMAGE), ('pull', '--platform', 'linux/amd64', self.IMAGE)])

    def test_the_hosts_own_variant_is_left_alone(self):
        calls = self.ensure(host='amd64', local_arch='amd64')
        self.assertFalse([c for c in calls if c[0] == 'pull'])

    def test_a_single_arch_image_is_pulled_back_and_falls_back_to_emulation(self):
        # no variant for this host: that pull fails, the image is pulled back as it was, and
        # start goes on under emulation (the start note explains)
        calls = self.ensure(host='amd64', local_arch='arm64', pull_ok=False)
        self.assertEqual([c for c in calls if c[0] in ('pull', 'image') and c[1] != 'inspect'],
                         [('image', 'rm', self.IMAGE), ('pull', '--platform', 'linux/amd64', self.IMAGE),
                          ('pull', self.IMAGE)])

    def test_a_missing_image_is_pulled_as_before(self):
        calls = self.ensure(host='amd64', local_arch='', present=False)
        self.assertEqual([c for c in calls if c[0] == 'pull'], [('pull', self.IMAGE)])
