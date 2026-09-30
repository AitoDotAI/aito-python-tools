"""The instance URL check both clients share (offline)"""
from aito.utils._generic_utils import instance_url_problem
from aito.v1.client import AitoClient, Error
from tests.cases import BaseTestCase


class TestInstanceUrl(BaseTestCase):
    def test_well_formed_urls_pass(self):
        for url in ('http://localhost:9005', 'https://shared.aito.ai/db/x', 'https://x.aito.app/'):
            self.assertIsNone(instance_url_problem(url), url)

    def test_urls_without_a_scheme_or_host_are_named(self):
        for url in ('localhost:9005', 'shared.aito.ai/db/x', 'ftp://x', 'http://', ''):
            problem = instance_url_problem(url)
            self.assertIn(f"'{url}'", problem)
            self.assertIn("'http://127.0.0.1:9005'", problem)

    def test_v1_client_raises_its_own_error_before_the_credential_check(self):
        # not the catch-all "please check your credentials"
        with self.assertRaises(Error) as ctx:
            AitoClient('localhost:9005', 'k')
        self.assertIn("'http://127.0.0.1:9005'", str(ctx.exception))
