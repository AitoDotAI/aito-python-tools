"""A release names the engine `aito start` runs; the pin must be that engine (offline)

The newest changelog section says which engine it pins ("runs engine **vX.Y.Z**"), and
``PINNED_IMAGE`` must be that tag, pinned by digest. So a release cannot ship claiming one
engine while `aito start` pulls another: re-pin with ``scripts/pin-engine.py <tag>``.
"""

import re
from pathlib import Path

from aito.local.server import PINNED_IMAGE
from tests.cases import BaseTestCase

CHANGELOG = Path(__file__).resolve().parents[2] / 'docs' / 'source' / 'changelog.rst'


def newest_section(text: str) -> str:
    heads = [m.start() for m in re.finditer(r'^(?:Unreleased|\d+\.\d+\.\d+)\n-+\n', text, re.M)]
    return text[heads[0]:heads[1] if len(heads) > 1 else None]


class TestReleasePin(BaseTestCase):
    def test_the_pin_is_a_tag_and_a_digest(self):
        self.assertRegex(PINNED_IMAGE, r'^ghcr\.io/aitohq/aito:v\d+\.\d+\.\d+@sha256:[0-9a-f]{64}$')

    def test_the_pin_is_the_engine_the_changelog_names(self):
        named = re.search(r'runs engine \*\*(v\d+\.\d+\.\d+)\*\*', newest_section(CHANGELOG.read_text()))
        if named is None:
            self.skipTest('the newest changelog section names no engine')
        pinned = PINNED_IMAGE.split(':', 2)[1].split('@')[0]
        self.assertEqual(
            pinned, named.group(1),
            f"the changelog says `aito start` runs engine {named.group(1)}, but PINNED_IMAGE is {pinned}: "
            f"run `python scripts/pin-engine.py {named.group(1)}` once that image is published")
