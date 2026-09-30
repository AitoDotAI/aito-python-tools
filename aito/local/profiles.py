"""Where an Aito instance's URL and key come from when the caller does not say

One store, shared with ``aito configure``: ``~/.config/aito/credentials``, an INI file
with one section per profile (``instance_url``, ``api_key``, and for a profile written
by ``aito start`` also ``read_only_api_key`` and the container it manages). Which
profile is active is recorded separately in ``~/.config/aito/config``, so a profile
section never has to share its namespace with settings.

Resolution, for both ``aito.Client()`` and the CLI, highest first:

1. explicit arguments (``Client(url, key)``, ``--instance-url``/``--api-key``)
2. environment: ``AITO_URL`` (or the older ``AITO_INSTANCE_URL``) and ``AITO_API_KEY``
3. the active profile: ``AITO_PROFILE`` if set, else the one ``aito start`` or
   ``aito profile use`` recorded, else ``default``

A key is never paired with a URL it was not stored with. A URL given without a key
takes the key of the profile stored for that same URL, or fails; a key given without
a URL fails rather than being sent to whatever the profile points at.
"""

import configparser
import os
import stat
from pathlib import Path
from typing import Dict, Optional, Tuple

CONFIG_DIR = Path(os.environ.get('XDG_CONFIG_HOME') or Path.home() / '.config') / 'aito'
CREDENTIALS_FILE = CONFIG_DIR / 'credentials'
SETTINGS_FILE = CONFIG_DIR / 'config'
DEFAULT_PROFILE = 'default'

URL_ENV_VARS = ('AITO_URL', 'AITO_INSTANCE_URL')
KEY_ENV_VAR = 'AITO_API_KEY'
PROFILE_ENV_VAR = 'AITO_PROFILE'


class NoCredentialsError(ValueError):
    """no instance URL and key could be resolved; the message names every way to give one"""


def _read(path: Path) -> configparser.ConfigParser:
    config = configparser.ConfigParser(interpolation=None)
    if path.exists():
        config.read(str(path))
    return config


def _write_private(path: Path, config: configparser.ConfigParser) -> None:
    """write an INI file readable by its owner only (0600, directory 0700)

    The file holds API keys. It is created with the final mode rather than chmod-ed
    after the write, so there is no window in which another user can read it.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, stat.S_IRWXU)
    tmp = path.with_name(path.name + '.tmp')
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        config.write(f)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def active_profile_name() -> str:
    """the profile used when no argument or environment variable names an instance"""
    name = os.environ.get(PROFILE_ENV_VAR)
    if name:
        return name
    return _read(SETTINGS_FILE).get('aito', 'active_profile', fallback=DEFAULT_PROFILE)


def set_active_profile(name: str) -> None:
    config = _read(SETTINGS_FILE)
    if not config.has_section('aito'):
        config.add_section('aito')
    config.set('aito', 'active_profile', name)
    _write_private(SETTINGS_FILE, config)


def load_profile(name: str) -> Optional[Dict[str, str]]:
    config = _read(CREDENTIALS_FILE)
    return dict(config[name]) if config.has_section(name) else None


def profiles() -> Dict[str, Dict[str, str]]:
    config = _read(CREDENTIALS_FILE)
    return {name: dict(config[name]) for name in config.sections()}


def save_profile(name: str, values: Dict[str, str]) -> None:
    """create or update one profile, keeping every other profile and field as it was"""
    config = _read(CREDENTIALS_FILE)
    if not config.has_section(name):
        config.add_section(name)
    for key, value in values.items():
        config.set(name, key, value)
    _write_private(CREDENTIALS_FILE, config)


def _env_url() -> Optional[str]:
    for var in URL_ENV_VARS:
        if os.environ.get(var):
            return os.environ[var]
    return None


def resolve_credentials(
        instance_url: Optional[str] = None, api_key: Optional[str] = None,
) -> Tuple[str, str]:
    """the instance URL and API key to use, from arguments, environment, then the active profile

    :raises NoCredentialsError: nothing names an instance, or a key has no URL of its own
    """
    url = instance_url or _env_url()
    key = api_key or os.environ.get(KEY_ENV_VAR)
    if url and key:
        return url, key
    if key and not url:
        raise NoCredentialsError(
            "an API key was given but no instance URL: pass instance_url, or set AITO_URL. "
            "The key is not sent to the active profile's instance, which it may not belong to.")

    name = active_profile_name()
    profile = load_profile(name)
    if url:
        # A URL without a key: the key stored for that same URL, if any profile has it
        for candidate in ([profile] if profile else []) + list(profiles().values()):
            if candidate.get('instance_url', '').rstrip('/') == url.rstrip('/') and candidate.get('api_key'):
                return url, candidate['api_key']
        raise NoCredentialsError(
            f"no API key for {url}: pass api_key, set AITO_API_KEY, or store it in a profile "
            f"(`aito configure`, or `aito start` for a local instance)")
    if profile and profile.get('instance_url') and profile.get('api_key'):
        return profile['instance_url'], profile['api_key']
    raise NoCredentialsError(
        "no Aito instance configured. Either pass Client(instance_url, api_key), set AITO_URL and "
        "AITO_API_KEY, or run `aito start` to start a local instance and store its keys in a profile"
        + (f" (the active profile '{name}' does not exist in {CREDENTIALS_FILE})" if profile is None else ""))
