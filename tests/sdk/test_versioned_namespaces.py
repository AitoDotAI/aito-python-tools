"""`aito.v1`, `aito.v2` and the default `aito.Client` — see docs/versioned-namespaces.md

Import-state assertions run in a SUBPROCESS: by the time this module executes, other
tests have imported half the package, so an in-process check would pass regardless.
"""

import subprocess
import sys

from tests.cases import BaseTestCase


def _run(code: str) -> str:
    result = subprocess.run([sys.executable, '-W', 'always::DeprecationWarning', '-c', code],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise AssertionError(f'probe failed:\n{result.stderr[-2000:]}')
    return result.stdout.strip() + '\n---stderr---\n' + result.stderr


class TestTheDefault(BaseTestCase):
    def test_client_is_v2_from_1_0(self):
        # The default moves only on a MAJOR release: 0.x -> v1, 1.x -> v2.
        # Fails on 0.7.0, where aito.Client was aito.v1.Client.
        import aito
        import aito.v2
        self.assertEqual(aito.DEFAULT_API_VERSION, 'v2')
        self.assertIs(aito.Client, aito.v2.Client)

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


class TestRemovedPaths(BaseTestCase):
    """The pre-0.7 paths were deprecated in 0.7 and removed in 1.0.

    Importing one raises an ImportError that NAMES the replacement, not a bare
    ModuleNotFoundError: the upgrade should tell the caller exactly what to write.
    """

    CASES = [
        ('from aito.client import AitoClient', 'from aito.v1 import Client'),
        ('from aito.client.v2 import AitoClientV2', 'from aito.v2 import Client'),
        ('from aito.client.requests import PredictRequest', 'aito.v1.requests'),
        ('import aito.client.aito_client', 'from aito.v1 import Client'),
        ('import aito.api as aito_api', 'aito.v1.api'),
    ]

    def test_each_removed_path_names_its_replacement(self):
        for stmt, replacement in self.CASES:
            out = subprocess.run([sys.executable, '-c', stmt], capture_output=True, text=True)
            self.assertNotEqual(out.returncode, 0, stmt)
            self.assertIn('ImportError', out.stderr, stmt)
            self.assertNotIn('ModuleNotFoundError', out.stderr, stmt)
            self.assertIn(replacement, out.stderr, stmt)
            self.assertIn('aitoai<1', out.stderr, stmt)


class TestBareInstallMessages(BaseTestCase):
    def test_optional_import_names_the_extra(self):
        from aito.utils._optional import import_optional
        with self.assertRaises(ImportError) as ctx:
            import_optional('surely_not_an_installed_module_xyz', 'widgets')
        self.assertIn("pip install 'aitoai[cli]'", str(ctx.exception))
        self.assertIn('widgets', str(ctx.exception))
