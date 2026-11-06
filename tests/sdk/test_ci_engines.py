"""CI runs the live tests against the engine `aito start` pins, so a pin bump cannot leave it behind

The live tests run against an Aito inside the CircleCI job, over a matrix of engine tags. One
of them must be the tag ``PINNED_IMAGE`` names: that is the engine users get.
"""

import re
from pathlib import Path

from aito.local.server import PINNED_IMAGE
from tests.cases import BaseTestCase

CONFIG = Path(__file__).resolve().parents[2] / '.circleci' / 'config.yml'


class TestCiEngines(BaseTestCase):
    def test_the_matrix_includes_the_pinned_engine(self):
        pinned = re.search(r':(v[0-9.]+)@', PINNED_IMAGE).group(1)
        matrix = re.search(r'engine: \[([^\]]+)\]', CONFIG.read_text())
        self.assertIsNotNone(matrix, 'no engine matrix in .circleci/config.yml')
        engines = [e.strip() for e in matrix.group(1).split(',')]
        self.assertIn(pinned, engines,
                      f'`aito start` pins {pinned}; add it to the engine matrix in .circleci/config.yml')
