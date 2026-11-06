"""CI runs the live tests against the engine `aito start` pins, so a pin bump cannot leave it behind

The live tests run against an Aito inside the CircleCI job, over a matrix of engine tags. One
of them must be the tag ``PINNED_IMAGE`` names: that is the engine users get. And every job
that reaches an Aito has its own, so no job depends on what the shared instance holds that day.
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

    def test_every_job_that_reaches_an_aito_has_its_own(self):
        # what shared held on the day made the tests flake (#96, #99); run_shared_smoke is the one
        # deliberate check against it
        without_an_aito = {
            'run_dependencies_security_check',  # scans the venv, no Aito
            'run_sdk_and_cli_tests_win',  # its live tests skip (AITO_SKIP_LIVE_TESTS)
            'run_shared_smoke',  # the v1 file upload's S3 path, which only a cloud instance has
            'build_and_deploy_to_test_pypi',  # builds and uploads, no Aito
        }
        jobs = CONFIG.read_text().split('\njobs:\n', 1)[1]
        bodies = dict(re.findall(r'^  (\w+):\n((?:(?:    .*)?\n)*)', jobs, re.MULTILINE))
        self.assertIn('run_shared_smoke', bodies, 'could not read the jobs in .circleci/config.yml')
        for job, body in bodies.items():
            if job not in without_an_aito:
                with self.subTest(job):
                    self.assertIn('ghcr.io/aitohq/aito:', body,
                                  f'{job} has no Aito of its own: add the engine as a service container')
