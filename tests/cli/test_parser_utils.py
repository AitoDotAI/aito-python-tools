from unittest.mock import patch

from aito.cli.parser import ParseError, ArgParser, parse_env_variable, create_client_from_parsed_args, \
    get_credentials_file_config, write_credentials_file_profile
from aito.client import AitoClient, Error
from tests.cases import BaseTestCase, CompareTestCase


class TestParserUtils(BaseTestCase):
    def test_parse_env_variable(self):
        self.assertIsNone(parse_env_variable('RADIO_GA_GA'))
        with self.assertRaises(ParseError):
            parse_env_variable('RADIO_GA_GA', True)


class TestCredentialsConfig(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.input_folder = cls.input_folder.parent

    def assertConfigProfile(self, config, profile_name, instance_url, api_key):
        self.assertTrue(config.has_section(profile_name))
        self.assertEqual(config.get(profile_name, 'instance_url'), instance_url)
        self.assertEqual(config.get(profile_name, 'api_key'), api_key)

    def test_get_config(self):
        config = get_credentials_file_config(self.input_folder / 'sample_config')
        self.assertEqual(config.sections(), ['default', 'space_oddity'])
        self.assertConfigProfile(config, 'default', 'space_oddity', 'star_man')
        self.assertConfigProfile(config, 'space_oddity', 'ground_control', 'major_tom')

    def test_write_config(self):
        out_fp = self.output_folder / 'nested' / 'config'

        def clean_up_folder():
            if out_fp.exists():
                out_fp.unlink()
            if out_fp.parent.exists():
                out_fp.parent.rmdir()

        self.addCleanup(clean_up_folder)
        # write to a file with parents not created
        write_credentials_file_profile('space_oddity', 'ground_control', 'major_tom', out_fp)
        config = get_credentials_file_config(out_fp)
        self.assertEqual(config.sections(), ['space_oddity'])
        self.assertConfigProfile(config, 'space_oddity', 'ground_control', 'major_tom')
        # write to a new profile
        write_credentials_file_profile('star_man', 'dont_blow_it', 'lets_boogie', out_fp)
        config = get_credentials_file_config(out_fp)
        self.assertEqual(config.sections(), ['space_oddity', 'star_man'])
        self.assertConfigProfile(config, 'space_oddity', 'ground_control', 'major_tom')
        self.assertConfigProfile(config, 'star_man', 'dont_blow_it', 'lets_boogie')
        # overwrite a profile
        write_credentials_file_profile('star_man', 'heroes', 'just_for_one_day', out_fp)
        config = get_credentials_file_config(out_fp)
        self.assertEqual(config.sections(), ['space_oddity', 'star_man'])
        self.assertConfigProfile(config, 'space_oddity', 'ground_control', 'major_tom')
        self.assertConfigProfile(config, 'star_man', 'heroes', 'just_for_one_day')


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

        with patch('aito.cli.parser.DEFAULT_CREDENTIAL_FILE', self.input_folder / 'sample_config'):
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
        with patch('aito.cli.parser.DEFAULT_CREDENTIAL_FILE', self.input_folder / 'sample_config'):
            self.assertEqual(
                vars(AitoClient('ground_control', 'major_tom', False)),
                vars(create_client_from_parsed_args(expected_parsed_args, check_credentials=False))
            )

    def test_create_client_unknown_profile(self):
        self.stub_environment_variable('AITO_INSTANCE_URL', None)
        self.stub_environment_variable('AITO_API_KEY', None)
        with self.assertRaises(ParseError):
            with patch('aito.cli.parser.DEFAULT_CREDENTIAL_FILE', self.input_folder / 'sample_config'):
                create_client_from_parsed_args(vars(self.parser.parse_args(['--profile', 'random'])))

    def test_create_error_client(self):
        with self.assertRaises(Error):
            create_client_from_parsed_args(vars(self.parser.parse_args(['-i', 'some_url', '-k', 'some_key'])))