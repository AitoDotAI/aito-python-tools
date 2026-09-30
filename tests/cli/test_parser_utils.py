from unittest.mock import patch

from aito.cli.parser import ParseError, ArgParser, parse_env_variable, create_client_from_parsed_args
from aito.v1 import AitoClient, Error
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
            'profile': None, 'instance_url': 'some_url', 'api_key': 'some_key'
        }
        self.assertEqual(
            vars(self.parser.parse_args(['-i', 'some_url', '-k', 'some_key'])),
            expected_parsed_args
        )
        self.assertEqual(
            vars(AitoClient('some_url', 'some_key', False)),
            vars(create_client_from_parsed_args(expected_parsed_args, check_credentials=False))
        )

    def test_create_client_from_aito_url_env_var(self):
        # AITO_URL is the name `aito start` prints; it wins over AITO_INSTANCE_URL, as in the SDK
        self.stub_environment_variable('AITO_INSTANCE_URL', 'the_older_url')
        self.stub_environment_variable('AITO_URL', 'some_url')
        self.stub_environment_variable('AITO_API_KEY', 'some_key')
        self.assertEqual(
            vars(AitoClient('some_url', 'some_key', False)),
            vars(create_client_from_parsed_args(vars(self.parser.parse_args([])), check_credentials=False))
        )

    def test_create_client_from_env_var(self):
        expected_parsed_args = {'profile': None, 'instance_url': '.env', 'api_key': '.env'}
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

    def _profile_file(self, **fields):
        import configparser, tempfile
        from pathlib import Path
        config = configparser.ConfigParser()
        config['p'] = fields
        path = Path(tempfile.mkdtemp()) / 'credentials'
        with path.open('w') as f:
            config.write(f)
        return path

    def _resolve_with_aito_url(self, path):
        self.stub_environment_variable('AITO_INSTANCE_URL', None)
        self.stub_environment_variable('AITO_API_KEY', None)
        self.stub_environment_variable('AITO_URL', 'https://elsewhere.aito.app')
        with patch('aito.utils._credentials_file_utils.DEFAULT_CREDENTIAL_FILE', path):
            return create_client_from_parsed_args(vars(self.parser.parse_args(['--profile', 'p'])),
                                                  check_credentials=False)

    def test_a_local_server_key_is_not_sent_to_another_url(self):
        # AITO_URL names another instance and no key is given: the key `aito start` stored for
        # 127.0.0.1 must not be handed to that server
        path = self._profile_file(instance_url='http://127.0.0.1:9005', api_key='local-key', container='aito')
        with self.assertRaises(ParseError) as ctx:
            self._resolve_with_aito_url(path)
        self.assertIn('no API key for https://elsewhere.aito.app', str(ctx.exception))

    def test_a_configure_profile_resolves_as_before(self):
        # profiles from `aito configure` keep the old behaviour: URL and key resolved separately
        path = self._profile_file(instance_url='https://old.aito.app', api_key='cloud-key')
        client = self._resolve_with_aito_url(path)
        self.assertEqual((client.instance_url, client.api_key), ('https://elsewhere.aito.app', 'cloud-key'))

    def test_create_client_unknown_profile(self):
        self.stub_environment_variable('AITO_INSTANCE_URL', None)
        self.stub_environment_variable('AITO_API_KEY', None)
        with self.assertRaises(ParseError):
            with patch('aito.utils._credentials_file_utils.DEFAULT_CREDENTIAL_FILE', self.input_folder / 'sample_config'):
                create_client_from_parsed_args(vars(self.parser.parse_args(['--profile', 'random'])))

    def test_create_error_client(self):
        with self.assertRaises(Error):
            create_client_from_parsed_args(vars(self.parser.parse_args(['-i', 'some_url', '-k', 'some_key'])))