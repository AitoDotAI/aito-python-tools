from aito.cli.parser import ParseError, ArgParser
from aito.cli.parser_utils import parse_env_variable, create_client_from_parsed_args
from aito.sdk.aito_client import AitoClient, BaseError
from tests.cases import CompareTestCase


class TestParserUtils(CompareTestCase):
    def test_parse_env_variable(self):
        self.assertIsNone(parse_env_variable('RADIO_GA_GA'))
        with self.assertRaises(ParseError):
            parse_env_variable('RADIO_GA_GA', True)


class TestCreateClientFromParsedArgs(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.parser = ArgParser()
        cls.parser.add_aito_default_credentials_arguments()

    def test_create_client_from_env_file(self):
        expected_parsed_args = {
            'use_env_file': self.input_folder / 'sample.env', 'instance_url': '.env', 'api_key': '.env'
        }
        self.assertEqual(
            vars(self.parser.parse_args(['-e', str(self.input_folder / 'sample.env')])),
            expected_parsed_args
        )
        self.stub_or_delete_environment_variable('AITO_INSTANCE_URL')
        self.stub_or_delete_environment_variable('AITO_API_KEY')
        self.assertEqual(
            vars(AitoClient('some_url', 'some_key', False)),
            vars(create_client_from_parsed_args(expected_parsed_args, check_credentials=False))
        )

    def test_create_client_from_flags(self):
        expected_parsed_args = {'use_env_file': None, 'instance_url': '.env', 'api_key': '.env'}
        self.assertEqual(
            vars(self.parser.parse_args([])),
            expected_parsed_args
        )
        self.stub_or_delete_environment_variable('AITO_INSTANCE_URL', 'some_url')
        self.stub_or_delete_environment_variable('AITO_API_KEY', 'some_key')
        self.assertEqual(
            vars(AitoClient('some_url', 'some_key', False)),
            vars(create_client_from_parsed_args(expected_parsed_args, check_credentials=False))
        )

    def test_create_client_non_exist_env_file(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args(['-e', str(self.input_folder / 'some_env_file.env')])

    def test_create_client_non_exist_env_var(self):
        self.stub_or_delete_environment_variable('AITO_INSTANCE_URL')
        self.stub_or_delete_environment_variable('AITO_API_KEY')
        with self.assertRaises(ParseError):
            create_client_from_parsed_args(vars(self.parser.parse_args([])))

    def test_create_error_client(self):
        with self.assertRaises(BaseError):
            create_client_from_parsed_args(vars(self.parser.parse_args(['-i', 'some_url', '-k', 'some_key'])))