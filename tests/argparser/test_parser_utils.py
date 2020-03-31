from aito.cli.parser_utils import *
from tests.cases import CompareTestCase


class TestParserUtils(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'

    def test_parse_env_variable(self):
        self.assertIsNone(parse_env_variable('RADIO_GA_GA'))
        with self.assertRaises(ParseError):
            parse_env_variable('RADIO_GA_GA', True)
