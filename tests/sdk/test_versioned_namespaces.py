"""`aito.v1`, `aito.v2` and the default `aito.Client` — see docs/versioned-namespaces.md

Import-state assertions run in a SUBPROCESS: by the time this module executes, other
tests have imported half the package, so an in-process check would pass regardless.
"""

import subprocess
import sys
import warnings

from tests.cases import BaseTestCase


def _run(code: str) -> str:
    result = subprocess.run([sys.executable, '-W', 'always::DeprecationWarning', '-c', code],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise AssertionError(f'probe failed:\n{result.stderr[-2000:]}')
    return result.stdout.strip() + '\n---stderr---\n' + result.stderr


class TestTheDefault(BaseTestCase):
    def test_client_is_v1_until_the_1_0_flip(self):
        # The default moves only on a MAJOR release: 0.x -> v1, 1.x -> v2.
        import aito
        import aito.v1
        self.assertEqual(aito.DEFAULT_API_VERSION, 'v1')
        self.assertIs(aito.Client, aito.v1.Client)

    def test_the_package_major_says_which_api_is_the_default(self):
        """Moving the default requires a major bump, and a major bump moves it.

        Only meaningful for a real release. CI's test.pypi dev builds are versioned by
        timestamp (`scripts/deploy --bump-version dev` -> 2026.9.25.12.0.0.dev), so
        their "major" is the year and says nothing about the API.
        """
        import aito
        from packaging.version import Version
        version = Version(aito.__version__)
        if version.is_devrelease:
            self.skipTest(f'{version} is a timestamped dev build')
        expected = {0: 'v1', 1: 'v2'}.get(version.major)
        self.assertEqual(aito.DEFAULT_API_VERSION, expected,
                         f'aitoai {version} must default to {expected}: moving '
                         f'DEFAULT_API_VERSION requires a major bump, and vice versa')

    def test_importing_aito_loads_no_api_version(self):
        # `aito.Client` is resolved lazily, or `import aito.v2` would drag v1 in.
        out = _run('import sys, aito; '
                   'print(sorted(m for m in sys.modules if m.startswith(("aito.v1","aito.v2","aito.client"))))')
        self.assertTrue(out.startswith('[]'), out)

    def test_importing_v2_loads_no_v1(self):
        out = _run('import sys, aito.v2; '
                   'print(sorted(m for m in sys.modules if m.startswith(("aito.v1","aito.client"))))')
        self.assertTrue(out.startswith('[]'), out)

    def test_unknown_attribute_still_raises(self):
        import aito
        with self.assertRaises(AttributeError):
            aito.NoSuchThing


class TestCanonicalNames(BaseTestCase):
    def test_v2_names(self):
        import aito.v2
        self.assertIs(aito.v2.Client, aito.v2.AitoClientV2)
        self.assertIs(aito.v2.Error, aito.v2.AitoV2Error)
        self.assertIn('Client', aito.v2.__all__)

    def test_v1_names(self):
        import aito.v1
        self.assertIs(aito.v1.Client, aito.v1.AitoClient)


class TestDeprecatedPaths(BaseTestCase):
    """Every pre-0.7 import keeps resolving — to the SAME object — and warns once.

    `aito.client` is frozen to v1 on purpose: code that says `aito.client` means v1,
    and must not be switched to v2 underneath it when the default moves.
    """

    def test_old_paths_resolve_to_the_same_objects(self):
        out = _run(
            'import sys, aito.v1, aito.v2\n'
            'from aito.client import AitoClient, RequestError\n'
            'from aito.client.requests import PredictRequest\n'
            'import aito.client.requests.query_api_request as q\n'
            'import aito.client.aito_client as c\n'
            'from aito.client.responses import HitsResponse\n'
            'from aito.client.v2 import AitoClientV2\n'
            'import aito.client.v2.errors as e2\n'
            'import aito.api as api\n'
            'print(all([AitoClient is aito.v1.Client, PredictRequest is aito.v1.PredictRequest,\n'
            '  q is sys.modules["aito.v1.requests.query_api_request"], c is sys.modules["aito.v1.client"],\n'
            '  HitsResponse is aito.v1.HitsResponse, AitoClientV2 is aito.v2.Client,\n'
            '  e2 is aito.v2.errors, api is aito.v1.api]))\n')
        self.assertTrue(out.startswith('True'), out)

    def test_each_old_path_warns_and_names_its_replacement(self):
        for path, replacement in [('aito.client', 'aito.v1'),
                                  ('aito.client.v2', 'aito.v2'),
                                  ('aito.api', 'aito.v1.api')]:
            out = _run(f'import {path}')
            self.assertIn('DeprecationWarning', out, path)
            self.assertIn(f'`{replacement}`', out, path)
            self.assertIn('aitoai 1.0', out, path)

    def test_aito_client_does_not_follow_the_default(self):
        import aito.v1
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            import aito.client
        self.assertIs(aito.client.AitoClient, aito.v1.Client)


class TestBareInstallMessages(BaseTestCase):
    def test_optional_import_names_the_extra(self):
        from aito.utils._optional import import_optional
        with self.assertRaises(ImportError) as ctx:
            import_optional('surely_not_an_installed_module_xyz', 'widgets')
        self.assertIn("pip install 'aitoai[cli]'", str(ctx.exception))
        self.assertIn('widgets', str(ctx.exception))
