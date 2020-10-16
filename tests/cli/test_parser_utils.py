from unittest.mock import patch

from aito.cli.parser import ParseError, ArgParser, parse_env_variable, create_client_from_parsed_args
from aito.client import AitoClient, Error
from tests.cases import BaseTestCase, CompareTestCase


class TestParserUtils(BaseTestCase):
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
        cls.input_folder = cls.input_folder.parent

    def test_create_client_from_flag(self):
        expected_parsed_args = {
            'profile': 'default', 'instance_url': 'some_url', 'api_key': 'some_key'
        }
        self.assertEqual(
            vars(self.parser.parse_args(['-i', 'some_url', '-k', 'some_key'])),
            expected_parsed_args
        )
        self.assertEqual(
            vars(AitoClient('some_url', 'some_key', False)),
            vars(create_client_from_parsed_args(expected_parsed_args, check_credentials=False))
        )

    def test_create_client_from_env_var(self):
        expected_parsed_args = {'profile': 'default', 'instance_url': '.env', 'api_key': '.env'}
        self.assertEqual(
            vars(self.parser.parse_args([])),
            expected_parsed_args
        )
        self.stub_environment_variable('AITO_INSTANCE_URL', 'some_url')
        self.stub_environment_variable('AITO_API_KEY', 'some_key')
        self.assertEqual(
            vars(AitoClient('some_url', 'some_key', False)),
            vars(create_client_from_parsed_args(expected_parsed_args, check_credentials=False))
        )

    def test_create_client_no_env_var_no_config(self):
        self.stub_environment_variable('AITO_INSTANCE_URL', None)
        self.stub_environment_variable('AITO_API_KEY', None)
        with self.assertRaises(ParseError):
            create_client_from_parsed_args(vars(self.parser.parse_args([])))

    def test_create_client_default_profile(self):
        self.stub_environment_variable('AITO_INSTANCE_URL', None)
        self.stub_environment_variable('AITO_API_KEY', None)

        with patch('aito.utils._credentials_file_utils.DEFAULT_CREDENTIAL_FILE', self.input_folder / 'sample_config'):
            self.assertEqual(
                vars(AitoClient('space_oddity', 'star_man', False)),
                vars(create_client_from_parsed_args(vars(self.parser.parse_args([])), check_credentials=False))
            )

    def test_create_client_select_profile(self):
        expected_parsed_args = {'profile': 'space_oddity', 'instance_url': '.env', 'api_key': '.env'}
        self.assertEqual(
            vars(self.parser.parse_args(['--profile', 'space_oddity'])),
            expected_parsed_args
        )
        self.stub_environment_variable('AITO_INSTANCE_URL', None)
        self.stub_environment_variable('AITO_API_KEY', None)
        with patch('aito.utils._credentials_file_utils.DEFAULT_CREDENTIAL_FILE', self.input_folder / 'sample_config'):
            self.assertEqual(
                vars(AitoClient('ground_control', 'major_tom', False)),
                vars(create_client_from_parsed_args(expected_parsed_args, check_credentials=False))
            )

    def test_create_client_unknown_profile(self):
        self.stub_environment_variable('AITO_INSTANCE_URL', None)
        self.stub_environment_variable('AITO_API_KEY', None)
        with self.assertRaises(ParseError):
            with patch('aito.utils._credentials_file_utils.DEFAULT_CREDENTIAL_FILE', self.input_folder / 'sample_config'):
                create_client_from_parsed_args(vars(self.parser.parse_args(['--profile', 'random'])))

    def test_create_error_client(self):
        with self.assertRaises(Error):
            create_client_from_parsed_args(vars(self.parser.parse_args(['-i', 'some_url', '-k', 'some_key'])))