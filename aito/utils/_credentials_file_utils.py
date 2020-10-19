import configparser
import re
from pathlib import Path

DEFAULT_CREDENTIAL_FILE = Path.home() / '.config' / 'aito' / 'credentials'


def get_credentials_file_config(credentials_file_path=None):
    if not credentials_file_path:
        credentials_file_path = DEFAULT_CREDENTIAL_FILE
    config = configparser.ConfigParser()
    try:
        config.read(str(credentials_file_path))
    except Exception as e:
        raise RuntimeError(f"failed to parse credentials file: {e}\n"
                         f"Please edit or delete the credentials file then run `aito configure`")
    return config


def get_existing_credentials(profile_name: str):
    config = get_credentials_file_config()
    if profile_name not in config.sections():
        return None, None
    else:
        return config.get(profile_name, 'instance_url', fallback=None), \
               config.get(profile_name, 'api_key', fallback=None)


def write_credentials_file_profile(
        profile_name, instance_url, api_key, credentials_file_path=None
):
    if not credentials_file_path:
        credentials_file_path = DEFAULT_CREDENTIAL_FILE

    if not credentials_file_path.exists():
        if not credentials_file_path.parent.exists():
            credentials_file_path.parent.mkdir(parents=True)
        existing_config = configparser.ConfigParser()
    else:
        existing_config = get_credentials_file_config(credentials_file_path)
    if not existing_config.has_section(profile_name):
         existing_config.add_section(profile_name)
    existing_config.set(section=profile_name, option='instance_url', value=instance_url)
    existing_config.set(section=profile_name, option='api_key', value=api_key)
    with credentials_file_path.open('w') as f:
        existing_config.write(f)


def mask_instance_url(instance_url: str):
    pattern = re.compile('^https://(.+?)\.(.+)$')
    matched = pattern.search(instance_url)
    if matched is None:
        return '****' if len(instance_url) <= 4 else (len(instance_url) - 4) * '*' + instance_url[-4:]
    else:
        instance_name = matched.group(1)
        return f"https://{(len(instance_name) - 4) * '*' + instance_name[-4:]}.{matched.group(2)}"


def mask_api_key(api_key: str):
    return '****' if len(api_key) <= 4 else (len(api_key) - 4) * '*' + api_key[-4:]
