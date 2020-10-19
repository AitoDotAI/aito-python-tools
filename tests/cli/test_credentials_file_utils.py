from aito.utils._credentials_file_utils import get_credentials_file_config, write_credentials_file_profile, mask_instance_url, mask_api_key
from parameterized import parameterized
from tests.cases import BaseTestCase, CompareTestCase


class TestCredentialsConfigFile(CompareTestCase):
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


class TestMaskCredentials(BaseTestCase):
    @parameterized.expand([
        ('old_prod_url', 'https://some-name.api.aito.ai', 'https://*****name.api.aito.ai'),
        ('new_prod_url', 'https://some-name.aito.app', 'https://*****name.aito.app'),
        ('old_test_url', 'https://some-name.api.aito.ninja', 'https://*****name.api.aito.ninja'),
        ('new_test_url', 'https://some-name.predictive.rocks', 'https://*****name.predictive.rocks'),
        ('incorrect_test_url', 'http://some-name.predictive.rocks', '*****************************ocks')
    ])
    def test_mask_instance_url(self, _, instance_url, expected_masked_instance_url):
        self.assertEqual(mask_instance_url(instance_url=instance_url), expected_masked_instance_url)

    @parameterized.expand([
        ('key', 'random-key', '******-key')
    ])
    def test_mask_api_key(self, _, api_key, expected_masked_api_key):
        self.assertEqual(mask_api_key(api_key=api_key), expected_masked_api_key)
