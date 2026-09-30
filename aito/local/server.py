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
#: there keeps their data when they switch to ``aito start``.
DEFAULT_VOLUME = 'aito-state'
DEFAULT_CONTAINER = 'aito'
DEFAULT_PROFILE = 'local'
DEFAULT_PORT = 9005
DEFAULT_SQL_PORT = 5432
#: Marks the containers this tool may replace. One it did not create is never touched.
MANAGED_LABEL = 'ai.aito.managed-by=aitoai-cli'
#: Which profile a managed container belongs to: another profile never replaces it.
PROFILE_LABEL = 'ai.aito.profile'
KEY_FILE = '/io/state/.aito-api-keys'
#: What a profile written by `aito start` holds; `from_profile` refuses one missing any.
PROFILE_FIELDS = ('instance_url', 'api_key', 'read_only_api_key', 'container', 'volume', 'image', 'port', 'sql_port')


def default_container(profile: str) -> str:
    """`aito` for the default profile, `aito-<profile>` otherwise, so profiles never share one"""
    return DEFAULT_CONTAINER if profile == DEFAULT_PROFILE else f'aito-{profile}'


def default_volume(profile: str) -> str:
    return DEFAULT_VOLUME if profile == DEFAULT_PROFILE else f'aito-{profile}-state'


def say(message: str) -> None:
    """print now: docker's own output (a pull) goes straight to the terminal, and a buffered
    line of ours would otherwise appear after it"""
    print(message, flush=True)


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
        # 127.0.0.1, not localhost: the ports are published on IPv4 loopback only, and a
        # client that resolves localhost to ::1 first (macOS, some Linux) is refused there
        return f'http://127.0.0.1:{self.port}'

    @classmethod
    def from_profile(cls, name: str) -> 'ServerConfig':
        """the server a profile describes; raises when there is none, or it is incomplete"""
        p = profiles.load_profile(name)
        again = "`aito start`" + (f" --profile {name}" if name != DEFAULT_PROFILE else "")
        if not p or 'container' not in p:
            raise LocalServerError(f"no local server profile '{name}'. Start one with {again}")
        missing = [f for f in PROFILE_FIELDS if not p.get(f)]
        if missing:
            raise LocalServerError(
                f"profile '{name}' is missing {', '.join(missing)} (edited by hand, or written by "
                f"`aito configure`?). {again} fills them in again.")
        try:
            return cls(profile=name, container=p['container'], volume=p['volume'], image=p['image'],
                       port=int(p['port']), sql_port=int(p['sql_port']))
        except ValueError:
            raise LocalServerError(f"profile '{name}' has a non-numeric port; {again} fixes it")

    @classmethod
    def for_start(cls, name: str, **given) -> 'ServerConfig':
        """what `aito start` runs: the options given, else the profile's stored values, else defaults

        So a rerun keeps the image, ports and names it ran with: only `aito upgrade` moves
        the image, and a fallback SQL port stays put.
        """
        stored = profiles.load_profile(name) or {}
        if stored.get('instance_url') and 'container' not in stored:
            # a profile from `aito configure` (a cloud or other instance): never overwrite it
            raise LocalServerError(
                f"profile '{name}' already holds credentials for {stored['instance_url']} (from `aito "
                f"configure`); `aito start` would overwrite them. Use another --profile.")

        def pick(key, default):
            if given.get(key) is not None:
                return given[key]
            return stored.get(key) or default

        # A new profile other than `local` starts on the first free port after 9005, so it
        # never collides with the `local` server (running or not yet started)
        first_port = DEFAULT_PORT
        if name != DEFAULT_PROFILE and given.get('port') is None and not stored.get('port'):
            first_port = next((p for p in range(DEFAULT_PORT + 1, DEFAULT_PORT + 50) if _port_free(p)),
                              DEFAULT_PORT + 1)
        try:
            cfg = cls(profile=name,
                      container=pick('container', default_container(name)),
                      volume=pick('volume', default_volume(name)),
                      image=pick('image', PINNED_IMAGE),
                      port=int(pick('port', first_port)),
                      sql_port=int(pick('sql_port', DEFAULT_SQL_PORT)))
        except ValueError:
            raise LocalServerError(f"profile '{name}' has a non-numeric port; pass --port / --sql-port")
        for what, port in (('--port', cfg.port), ('--sql-port', cfg.sql_port)):
            if not 0 < port < 65536:
                raise LocalServerError(f"{what} {port} is not a usable port (1-65535)")
        if not cfg.container or not cfg.volume or not cfg.image:
            raise LocalServerError("--container, --volume and --image may not be empty")
        return cfg


def stored_keys(name: str) -> Dict[str, str]:
    p = profiles.load_profile(name) or {}
    if not p.get('api_key') or not p.get('read_only_api_key'):
        raise LocalServerError(
            f"profile '{name}' holds no complete key pair; `aito start` stores one, "
            f"`aito keys --rotate` makes a new one")
    return {'api_key': p['api_key'], 'read_only_api_key': p['read_only_api_key']}


def _docker(*args: str, check: bool = True, capture: bool = True) -> subprocess.CompletedProcess:
    try:
        res = subprocess.run(['docker', *args], text=True,
                             stdout=subprocess.PIPE if capture else None,
                             stderr=subprocess.PIPE if capture else None)
    except FileNotFoundError:
        raise LocalServerError(
            "Docker is not installed. `aito start` runs Aito in Docker: install Docker Desktop "
            "(macOS, Windows) or Docker Engine (Linux), https://docs.docker.com/get-docker/")
    if check and res.returncode != 0:
        err = (res.stderr or '').strip() or f'exit {res.returncode}'
        raise LocalServerError(f"docker {args[0]} failed: {err}")
    return res


def check_docker() -> None:
    if shutil.which('docker') is None:
        _docker('version')  # raises the install hint
    # No --format: podman-as-docker has no .ServerVersion, and only the exit code matters here
    res = _docker('info', check=False)
    if res.returncode != 0:
        raise LocalServerError(
            "Docker is installed but its daemon is not reachable. Start Docker Desktop, or on Linux "
            "`sudo systemctl start docker` (and make sure your user may use it: `docker info`).\n"
            f"docker said: {(res.stderr or '').strip()}")
    # Docker Desktop on Windows can be in Windows-containers mode, where a Linux image cannot run
    os_type = _docker('version', '--format', '{{.Server.Os}}', check=False).stdout.strip().lower()
    if os_type == 'windows':
        raise LocalServerError(
            "Docker is running Windows containers, and Aito is a Linux image. Switch Docker Desktop to "
            "Linux containers (tray icon, \"Switch to Linux containers...\"), then run `aito start` again.")


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


def _exited(container: str) -> Optional[str]:
    """None while the container runs; else why it stopped, with the last lines of its log"""
    info = _inspect(container)
    if info is None:
        return "the container no longer exists (removed while starting?)"
    state = info['State']
    # --restart unless-stopped restarts a crashing container, so "running" alone hides the
    # crash: a restart before the server ever answered is the same failure
    crashed = state.get('Restarting') or info.get('RestartCount', 0) > 0
    if state.get('Running') and not crashed:
        return None
    tail = _docker('logs', '--tail', '15', container, check=False)
    log = ((tail.stdout or '') + (tail.stderr or '')).strip()
    hint = ""
    if 'exec format error' in log:
        hint = ("\nThe image is for another CPU architecture and this host cannot emulate it. On an arm64 "
                "Linux host, install emulation (`docker run --privileged --rm tonistiigi/binfmt --install amd64`) "
                "or wait for the multi-arch image.")
    how = (f"restarted {info.get('RestartCount', 0)} time(s)" if crashed else f"exit code {state.get('ExitCode')}")
    return f"{how}:\n{log}{hint}"


#: A native start is ~5 s on Linux, but ~70 s inside colima's VM and longer under CPU
#: emulation (measured in CI, .github/workflows/local-server.yml), so the wait is generous
#: and reports progress rather than sitting silent.
STARTUP_TIMEOUT = 300.0


def wait_healthy(url: str, api_key: str, timeout: float = STARTUP_TIMEOUT, container: Optional[str] = None,
                 log=None) -> float:
    """seconds until /version answers and the key is accepted; raises on timeout, a refused key,
    or (given the container) as soon as the container stops"""
    start = time.monotonic()
    next_note = 15.0
    while _get(f'{url}/version') != 200:
        if container and (why := _exited(container)):
            raise LocalServerError(f"the Aito container stopped during startup, {why}")
        waited = time.monotonic() - start
        if waited > timeout:
            raise LocalServerError(
                f"Aito did not answer on {url} within {timeout:.0f}s. See `aito logs`.")
        if log and waited > next_note:
            log(f"  still starting ({waited:.0f}s) ...")
            next_note += 15.0
        time.sleep(0.5)
    # The first authenticated request can be slow on a slow host (colima's VM took more
    # than 3 s): no answer, or a 5xx, is "not ready yet". Only 401/403 mean the key is wrong.
    while True:
        status = _get(f'{url}/api/v2/schema', api_key, timeout=30.0)
        if status == 200:
            return time.monotonic() - start
        if status in (401, 403):
            raise LocalServerError(
                f"Aito is up but refused the stored key (HTTP {status}). If this volume was started "
                f"with other keys, run `aito keys --rotate`.")
        if time.monotonic() - start > timeout:
            raise LocalServerError(
                f"Aito answered /version but not an authenticated request within {timeout:.0f}s "
                f"(last: {'no answer' if status == 0 else f'HTTP {status}'}). See `aito logs`.")
        time.sleep(1.0)


def _run_container(cfg: ServerConfig, keys: Dict[str, str]) -> None:
    # Keys go in through an env file, not -e arguments, so they never appear in the
    # process list. (docker inspect still shows them: the image takes keys by env.)
    fd, env_path = tempfile.mkstemp(prefix='aito-keys-', text=True)
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(f"READ_WRITE_APIKEY={keys['api_key']}\nAPIKEY={keys['read_only_api_key']}\n")
        _docker('run', '-d', '--name', cfg.container, '--label', MANAGED_LABEL,
                '--label', f'{PROFILE_LABEL}={cfg.profile}',
                '--restart', 'unless-stopped',
                '-p', f'127.0.0.1:{cfg.port}:9005', '-p', f'127.0.0.1:{cfg.sql_port}:5432',
                '-v', f'{cfg.volume}:/io/state', '--env-file', env_path, cfg.image)
    finally:
        os.unlink(env_path)


def _remove_managed(container: str, profile: str) -> None:
    info = _inspect(container)
    if info is None:
        return
    if not _is_managed(info):
        raise LocalServerError(
            f"a container named '{container}' exists that `aito start` did not create; it is left "
            f"alone. Remove it (`docker rm -f {container}`) or choose another name with --container.")
    owner = (info.get('Config', {}).get('Labels') or {}).get(PROFILE_LABEL)
    if owner and owner != profile:
        raise LocalServerError(
            f"container '{container}' belongs to profile '{owner}'; replacing it would break that "
            f"profile's keys. Use --profile {owner}, or give this profile its own --container.")
    _docker('rm', '-f', container)


def _profile_on_port(port: int) -> Optional[str]:
    """the profile whose managed container publishes this port, if any"""
    for name, p in profiles.profiles().items():
        if p.get('container') and p.get('port') == str(port):
            info = _inspect(p['container'])
            if info and info['State'].get('Running'):
                return name
    return None


def _running_with(cfg: ServerConfig, keys: Dict[str, str]) -> bool:
    """the managed container is up with exactly this image, these keys and these ports"""
    info = _inspect(cfg.container)
    if not info or not _is_managed(info) or not info['State'].get('Running'):
        return False
    env = dict(e.split('=', 1) for e in info['Config'].get('Env', []) if '=' in e)
    ports = info['HostConfig'].get('PortBindings') or {}
    host_port = lambda p: int((ports.get(p) or [{}])[0].get('HostPort') or 0)  # noqa: E731
    # by image ID, not by the reference string: podman normalises the reference it stores
    return (str(info.get('Image', '')).replace('sha256:', '') == _image_id(cfg.image)
            and env.get('READ_WRITE_APIKEY') == keys['api_key']
            and env.get('APIKEY') == keys['read_only_api_key']
            and host_port('9005/tcp') == cfg.port and host_port('5432/tcp') == cfg.sql_port)


def start(cfg: ServerConfig, activate: Optional[bool] = None, log=say) -> Dict:
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
        _remove_managed(cfg.container, cfg.profile)
        if not _image_present(cfg.image):
            _pull(cfg.image, log)
        if not _port_free(cfg.port):
            # the hint repeats --profile: without it, following the hint would restart the
            # `local` server on the new port instead (fresh-eyes rerun, 30.9)
            profile_flag = f" --profile {cfg.profile}" if cfg.profile != DEFAULT_PROFILE else ""
            holder = _profile_on_port(cfg.port)
            by = f"by the local Aito of profile '{holder}'" if holder else "by something else"
            raise LocalServerError(
                f"port {cfg.port} on 127.0.0.1 is in use {by}. Pick another: "
                f"`aito start{profile_flag} --port {cfg.port + 1}`")
        if not _port_free(cfg.sql_port):
            free = next((p for p in range(cfg.sql_port + 1, cfg.sql_port + 20) if _port_free(p)), None)
            if free is None:
                raise LocalServerError(f"no free port for SQL near {cfg.sql_port}; pass --sql-port")
            notes.append(f"Port {cfg.sql_port} is taken (a local Postgres?), so SQL is on {free}.")
            cfg.sql_port = free
        emulated = _emulation_note(cfg.image)
        if emulated:
            log(f"note: {emulated}")
        _run_container(cfg, keys)
        # Stored BEFORE the wait: if startup fails, `aito logs`, `status` and `stop` can still
        # reach the container, and the keys it runs with are not lost
        _save(cfg, keys)
        seconds = wait_healthy(cfg.url, keys['api_key'], container=cfg.container, log=log)
        state = 'started'

    _save(cfg, keys)
    active = profiles.active_profile_name()
    if activate or (activate is None and profiles.load_profile(active) is None):
        profiles.set_active_profile(cfg.profile)
        active = cfg.profile
    return {'state': state, 'seconds': seconds, 'keys': keys, 'config': cfg,
            'active': active == cfg.profile, 'notes': notes}


def _save(cfg: ServerConfig, keys: Dict[str, str]) -> None:
    profiles.save_profile(cfg.profile, {
        'instance_url': cfg.url, **keys, 'container': cfg.container, 'volume': cfg.volume,
        'image': cfg.image, 'port': str(cfg.port), 'sql_port': str(cfg.sql_port)})


def _emulation_note(image: str) -> Optional[str]:
    """a warning when the image is for another CPU than the Docker host (Apple Silicon, arm64 Linux)"""
    host = _docker('version', '--format', '{{.Server.Arch}}', check=False).stdout.strip()
    img = _docker('image', 'inspect', '--format', '{{.Architecture}}', image, check=False).stdout.strip()
    if not host or not img or host == img or '<no value>' in (host + img):
        return None
    return (f"this image is {img}-only and this Docker host is {host}, so Docker has to emulate it: slower, and "
            f"the first start can take a few minutes (a host without {img} emulation fails with 'exec format "
            f"error'). A native {host} image comes with the multi-arch release.")


def _image_id(image: str) -> str:
    res = _docker('image', 'inspect', '--format', '{{.Id}}', image, check=False)
    return res.stdout.strip().replace('sha256:', '')


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
        raise LocalServerError(f"'{cfg.container}' was not started by `aito start`; not stopping it")
    owner = (info.get('Config', {}).get('Labels') or {}).get(PROFILE_LABEL)
    if owner and owner != cfg.profile:
        raise LocalServerError(f"'{cfg.container}' belongs to profile '{owner}'; use `aito stop --profile {owner}`")
    _docker('stop', cfg.container)
    return True


def rotate_keys(cfg: ServerConfig) -> Dict:
    """new keys: recreate the container with them and store them; the data stays

    The container is recreated to take the new keys, so a stopped server runs again
    afterwards; `was_running` lets the caller say so.
    """
    check_docker()
    info = _inspect(cfg.container)
    was_running = bool(info and info['State'].get('Running'))
    keys = _new_keys()
    _remove_managed(cfg.container, cfg.profile)
    _run_container(cfg, keys)
    profiles.save_profile(cfg.profile, keys)
    wait_healthy(cfg.url, keys['api_key'], container=cfg.container)
    return {'keys': keys, 'was_running': was_running}


def upgrade(cfg: ServerConfig, image: str = PINNED_IMAGE, log=say) -> Dict:
    """move the container to `image` (default: the one this SDK version pins), same keys and data"""
    check_docker()
    keys = stored_keys(cfg.profile)
    before = cfg.image
    cfg.image = image
    if not _image_present(image):
        _pull(image, log)
    _remove_managed(cfg.container, cfg.profile)
    _run_container(cfg, keys)
    profiles.save_profile(cfg.profile, {'image': image})
    wait_healthy(cfg.url, keys['api_key'], container=cfg.container)
    return {'from': before, 'to': image}
