"""A local Aito in Docker, managed for the user: start, inspect, stop, rotate keys, upgrade

Talks to the ``docker`` command rather than the Docker API, so it needs nothing beyond
the standard library and works wherever the user's own ``docker`` works.

The image contract it relies on (the free image, aito-core docker/free):

- ``READ_WRITE_APIKEY`` / ``APIKEY`` in the environment are the keys, read by the
  engine itself. When both are set the entrypoint generates nothing.
- ``GET /version`` answers 200 without a key once the server is up; an authenticated
  ``GET /api/v2/schema`` proves the key.
- HTTP on 9005, the Postgres wire on 5432, state on ``/io/state``.
"""

import json
import os
import secrets
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Dict, List, Optional

from . import profiles

#: The engine release this SDK version runs. Pinned by digest: a tag can be re-pointed,
#: a digest cannot. Moves with SDK releases; ``aito upgrade`` applies it.
PINNED_IMAGE = ('ghcr.io/aitohq/aito:v2.11.1'
                '@sha256:904295cf491f509de6996e1e104aa1b196a01b4c32e80ba6092c9dccf6612bf5')
#: The volume name aito.ai/docker already tells people to use, so a user who started
#: there keeps their data when they switch to ``aito serve``.
DEFAULT_VOLUME = 'aito-state'
DEFAULT_CONTAINER = 'aito'
DEFAULT_PROFILE = 'local'
DEFAULT_PORT = 9005
DEFAULT_SQL_PORT = 5432
#: Marks the containers this tool may replace. One it did not create is never touched.
MANAGED_LABEL = 'ai.aito.managed-by=aitoai-cli'
KEY_FILE = '/io/state/.aito-api-keys'


class LocalServerError(RuntimeError):
    """a step failed; the message says what to do"""


@dataclass
class ServerConfig:
    profile: str = DEFAULT_PROFILE
    container: str = DEFAULT_CONTAINER
    volume: str = DEFAULT_VOLUME
    image: str = PINNED_IMAGE
    port: int = DEFAULT_PORT
    sql_port: int = DEFAULT_SQL_PORT

    @property
    def url(self) -> str:
        return f'http://localhost:{self.port}'

    @classmethod
    def from_profile(cls, name: str) -> 'ServerConfig':
        p = profiles.load_profile(name)
        if not p or 'container' not in p:
            raise LocalServerError(
                f"no local server profile '{name}'. Start one with `aito serve`"
                + (f" --profile {name}" if name != DEFAULT_PROFILE else ""))
        return cls(profile=name, container=p['container'], volume=p['volume'], image=p['image'],
                   port=int(p['port']), sql_port=int(p['sql_port']))


def _docker(*args: str, check: bool = True, capture: bool = True) -> subprocess.CompletedProcess:
    try:
        res = subprocess.run(['docker', *args], text=True,
                             stdout=subprocess.PIPE if capture else None,
                             stderr=subprocess.PIPE if capture else None)
    except FileNotFoundError:
        raise LocalServerError(
            "Docker is not installed. `aito serve` runs Aito in Docker: install Docker Desktop "
            "(macOS, Windows) or Docker Engine (Linux), https://docs.docker.com/get-docker/")
    if check and res.returncode != 0:
        err = (res.stderr or '').strip() or f'exit {res.returncode}'
        raise LocalServerError(f"docker {args[0]} failed: {err}")
    return res


def check_docker() -> None:
    if shutil.which('docker') is None:
        _docker('version')  # raises the install hint
    res = _docker('info', '--format', '{{.ServerVersion}}', check=False)
    if res.returncode != 0:
        raise LocalServerError(
            "Docker is installed but its daemon is not reachable. Start Docker Desktop, or on Linux "
            "`sudo systemctl start docker` (and make sure your user may use it: `docker info`).\n"
            f"docker said: {(res.stderr or '').strip()}")


def _inspect(container: str) -> Optional[Dict]:
    res = _docker('container', 'inspect', container, check=False)
    return json.loads(res.stdout)[0] if res.returncode == 0 else None


def _is_managed(info: Dict) -> bool:
    key, value = MANAGED_LABEL.split('=')
    return (info.get('Config', {}).get('Labels') or {}).get(key) == value


def _volume_exists(volume: str) -> bool:
    return _docker('volume', 'inspect', volume, check=False).returncode == 0


def _keys_on_volume(volume: str, image: str) -> Optional[Dict[str, str]]:
    """the keys an earlier plain `docker run` generated into this volume, if any

    Adopting them keeps every client that already uses them working. Read with a
    throwaway container, so nothing needs the volume's host path.
    """
    res = _docker('run', '--rm', '--entrypoint', 'cat', '-v', f'{volume}:/io/state', image, KEY_FILE,
                  check=False)
    if res.returncode != 0:
        return None
    keys = dict(line.split('=', 1) for line in res.stdout.splitlines() if '=' in line)
    if keys.get('READ_WRITE_APIKEY') and keys.get('APIKEY'):
        return {'api_key': keys['READ_WRITE_APIKEY'], 'read_only_api_key': keys['APIKEY']}
    return None


def _new_keys() -> Dict[str, str]:
    return {'api_key': secrets.token_hex(24), 'read_only_api_key': secrets.token_hex(24)}


def _port_free(port: int) -> bool:
    """whether Docker could publish this port on 127.0.0.1

    SO_REUSEADDR, as Docker's own proxy binds: without it, the TIME_WAIT sockets our
    health polls leave behind would make a just-stopped server's port look taken.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(('127.0.0.1', port))
            return True
        except OSError:
            return False


def _get(url: str, key: Optional[str] = None, timeout: float = 3.0) -> int:
    req = urllib.request.Request(url, headers={'x-api-key': key} if key else {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except (urllib.error.URLError, OSError):
        return 0


def wait_healthy(url: str, api_key: str, timeout: float = 120.0) -> float:
    """seconds until /version answers and the key is accepted; raises on timeout or a refused key"""
    start = time.monotonic()
    while _get(f'{url}/version') != 200:
        if time.monotonic() - start > timeout:
            raise LocalServerError(
                f"Aito did not answer on {url} within {timeout:.0f}s. See `aito logs`.")
        time.sleep(0.5)
    status = _get(f'{url}/api/v2/schema', api_key)
    if status != 200:
        raise LocalServerError(
            f"Aito is up but refused the stored key (HTTP {status}). If this volume was started "
            f"with other keys, run `aito keys --rotate`.")
    return time.monotonic() - start


def _run_container(cfg: ServerConfig, keys: Dict[str, str]) -> None:
    # Keys go in through an env file, not -e arguments, so they never appear in the
    # process list. (docker inspect still shows them: the image takes keys by env.)
    fd, env_path = tempfile.mkstemp(prefix='aito-keys-', text=True)
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(f"READ_WRITE_APIKEY={keys['api_key']}\nAPIKEY={keys['read_only_api_key']}\n")
        _docker('run', '-d', '--name', cfg.container, '--label', MANAGED_LABEL,
                '--restart', 'unless-stopped',
                '-p', f'127.0.0.1:{cfg.port}:9005', '-p', f'127.0.0.1:{cfg.sql_port}:5432',
                '-v', f'{cfg.volume}:/io/state', '--env-file', env_path, cfg.image)
    finally:
        os.unlink(env_path)


def _remove_managed(container: str) -> None:
    info = _inspect(container)
    if info is None:
        return
    if not _is_managed(info):
        raise LocalServerError(
            f"a container named '{container}' exists that `aito serve` did not create; it is left "
            f"alone. Remove it (`docker rm -f {container}`) or choose another name with --container.")
    _docker('rm', '-f', container)


def _running_with(cfg: ServerConfig, keys: Dict[str, str]) -> bool:
    """the managed container is up with exactly this image, these keys and these ports"""
    info = _inspect(cfg.container)
    if not info or not _is_managed(info) or not info['State'].get('Running'):
        return False
    env = dict(e.split('=', 1) for e in info['Config'].get('Env', []) if '=' in e)
    ports = info['HostConfig'].get('PortBindings') or {}
    host_port = lambda p: int((ports.get(p) or [{}])[0].get('HostPort') or 0)  # noqa: E731
    return (info['Config']['Image'] == cfg.image
            and env.get('READ_WRITE_APIKEY') == keys['api_key']
            and env.get('APIKEY') == keys['read_only_api_key']
            and host_port('9005/tcp') == cfg.port and host_port('5432/tcp') == cfg.sql_port)


def serve(cfg: ServerConfig, activate: Optional[bool] = None, log=print) -> Dict:
    """start (or confirm) the local server and store its profile; idempotent

    Keys, in order: the profile's (so a restart never changes them), the ones an earlier
    plain `docker run` generated into the same volume, else new ones.
    """
    check_docker()
    stored = profiles.load_profile(cfg.profile) or {}
    notes: List[str] = []
    if stored.get('api_key') and stored.get('read_only_api_key'):
        keys = {'api_key': stored['api_key'], 'read_only_api_key': stored['read_only_api_key']}
    else:
        keys = None
        if _volume_exists(cfg.volume):
            if not _image_present(cfg.image):
                _pull(cfg.image, log)
            keys = _keys_on_volume(cfg.volume, cfg.image)
            notes.append(f"Adopted the keys already stored in volume '{cfg.volume}'." if keys else
                         f"Volume '{cfg.volume}' exists (its data is kept) but holds no keys this tool can "
                         f"read, so new keys were generated; keys it was started with no longer work.")
        keys = keys or _new_keys()

    if _running_with(cfg, keys):
        seconds = wait_healthy(cfg.url, keys['api_key'])
        state = 'already running'
    else:
        _remove_managed(cfg.container)
        if not _image_present(cfg.image):
            _pull(cfg.image, log)
        if not _port_free(cfg.port):
            raise LocalServerError(
                f"port {cfg.port} on 127.0.0.1 is in use by something else. Pick another: "
                f"`aito serve --port {cfg.port + 1}`")
        if not _port_free(cfg.sql_port):
            free = next((p for p in range(cfg.sql_port + 1, cfg.sql_port + 20) if _port_free(p)), None)
            if free is None:
                raise LocalServerError(f"no free port for SQL near {cfg.sql_port}; pass --sql-port")
            notes.append(f"Port {cfg.sql_port} is taken (a local Postgres?), so SQL is on {free}.")
            cfg.sql_port = free
        _run_container(cfg, keys)
        seconds = wait_healthy(cfg.url, keys['api_key'])
        state = 'started'

    profiles.save_profile(cfg.profile, {
        'instance_url': cfg.url, **keys, 'container': cfg.container, 'volume': cfg.volume,
        'image': cfg.image, 'port': str(cfg.port), 'sql_port': str(cfg.sql_port)})
    active = profiles.active_profile_name()
    if activate or (activate is None and profiles.load_profile(active) is None):
        profiles.set_active_profile(cfg.profile)
        active = cfg.profile
    return {'state': state, 'seconds': seconds, 'keys': keys, 'config': cfg,
            'active': active == cfg.profile, 'notes': notes}


def _image_present(image: str) -> bool:
    return _docker('image', 'inspect', image, check=False).returncode == 0


def _pull(image: str, log) -> None:
    log(f"Pulling {image.split('@')[0]} (first run only) ...")
    _docker('pull', image, capture=False)


def status(cfg: ServerConfig) -> Dict:
    info = _inspect(cfg.container)
    p = profiles.load_profile(cfg.profile) or {}
    return {
        'container': ('missing' if info is None else info['State'].get('Status', 'unknown')),
        'image': (info or {}).get('Config', {}).get('Image', cfg.image),
        'version_http': _get(f'{cfg.url}/version'),
        'key_accepted': _get(f'{cfg.url}/api/v2/schema', p.get('api_key')) == 200 if p.get('api_key') else None,
        'config': cfg,
        'active': profiles.active_profile_name() == cfg.profile,
    }


def logs(cfg: ServerConfig, follow: bool = False, tail: int = 200) -> int:
    args = ['logs', '--tail', str(tail)] + (['-f'] if follow else []) + [cfg.container]
    return _docker(*args, check=False, capture=False).returncode


def stop(cfg: ServerConfig) -> bool:
    """stop the container; the volume, and so the data and keys, stay"""
    info = _inspect(cfg.container)
    if info is None or not info['State'].get('Running'):
        return False
    if not _is_managed(info):
        raise LocalServerError(f"'{cfg.container}' was not started by `aito serve`; not stopping it")
    _docker('stop', cfg.container)
    return True


def rotate_keys(cfg: ServerConfig) -> Dict[str, str]:
    """new keys: recreate the container with them and store them; the data stays"""
    check_docker()
    keys = _new_keys()
    _remove_managed(cfg.container)
    _run_container(cfg, keys)
    wait_healthy(cfg.url, keys['api_key'])
    profiles.save_profile(cfg.profile, keys)
    return keys


def upgrade(cfg: ServerConfig, image: str = PINNED_IMAGE, log=print) -> Dict:
    """move the container to `image` (default: the one this SDK version pins), same keys and data"""
    check_docker()
    p = profiles.load_profile(cfg.profile) or {}
    keys = {'api_key': p['api_key'], 'read_only_api_key': p['read_only_api_key']}
    before = cfg.image
    cfg.image = image
    if not _image_present(image):
        _pull(image, log)
    _remove_managed(cfg.container)
    _run_container(cfg, keys)
    wait_healthy(cfg.url, keys['api_key'])
    profiles.save_profile(cfg.profile, {'image': image})
    return {'from': before, 'to': image}
