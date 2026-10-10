"""One version across aitoai, aito-mcp, the registry entry and the changelog (scripts/versions.py)

The release workflow publishes two PyPI packages and a registry entry from one tag. If any of
them names another version, the release would publish a mismatched set: an aito-mcp that pins
an aitoai that does not exist, or a registry entry for a package version nobody uploaded.
"""

import importlib.util
import os
import shutil
import unittest
import tempfile
from pathlib import Path

from tests.cases import BaseTestCase

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('versions', ROOT / 'scripts' / 'versions.py')
versions = importlib.util.module_from_spec(spec)
spec.loader.exec_module(versions)


class TestReleaseVersions(BaseTestCase):
    def copy(self):
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp)
        for name in (versions.INIT, versions.MCP_PYPROJECT, versions.SERVER_JSON, versions.CHANGELOG):
            (tmp / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(ROOT / name, tmp / name)
        return tmp

    # The built-package CI jobs rewrite aito/__init__.py to a dev version before building
    # (scripts/deploy --bump-version dev), so the tree is inconsistent there by design.
    @unittest.skipIf(os.environ.get('TEST_BUILT_PACKAGE'), 'the built-package job bumps aitoai to a dev version')
    def test_the_repository_is_consistent(self):
        self.assertEqual(versions.problems(ROOT), [])

    def test_a_version_left_behind_is_caught(self):
        tmp = self.copy()
        path = tmp / versions.MCP_PYPROJECT
        path.write_text(path.read_text().replace('"aitoai[mcp]==', '"aitoai[mcp]==0.0.1+'))
        self.assertTrue(any('disagree' in p for p in versions.problems(tmp)))

    def test_bump_moves_everything_and_releases_the_unreleased_section(self):
        tmp = self.copy()
        changelog = tmp / versions.CHANGELOG
        if 'Unreleased\n----------' not in changelog.read_text():
            changelog.write_text(changelog.read_text().replace(
                'Changelog\n=========\n', 'Changelog\n=========\n\nUnreleased\n----------\n\n- a fix\n', 1))
        versions.bump(tmp, '9.8.7')
        found = versions.read(tmp)
        self.assertIn('9.8.7', found.pop('_changelog_sections'))
        self.assertEqual(set(found.values()), {'9.8.7'})
        self.assertEqual(versions.problems(tmp, tag='9.8.7'), [])

    def test_a_tag_that_is_not_the_version_is_refused(self):
        self.assertTrue(any('tag' in p for p in versions.problems(ROOT, tag='99.0.0')))

    def test_bump_refuses_a_non_version(self):
        with self.assertRaises(SystemExit):
            versions.bump(self.copy(), 'v1.2')
