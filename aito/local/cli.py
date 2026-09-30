"""``aito start | stop | status | logs | keys | upgrade | profile``: the local server commands

Dispatched from :func:`aito.cli.main` before the ``[cli]`` extra is imported, so they run
on a bare ``pip install aitoai``. The existing (v1) commands are untouched.
"""

import argparse
import sys
from typing import List, Optional

from . import profiles, server

COMMANDS = ('start', 'stop', 'status', 'logs', 'keys', 'upgrade', 'profile')
#: Words people and agents will guess, reserved for a possible FOREGROUND mode: `serve`,
#: `run` and `up` (compose stays attached without -d) all read as foreground, while
#: `start` returns once the server is up. They print a hint and fail rather than run
#: `start`, so no silent alias becomes behaviour we would have to keep. See ADR 0001.
RESERVED = ('serve', 'run', 'up')
RESERVED_HINT = ("`aito start` starts a local Aito in the background (see `aito start -h`); "
                 "a foreground mode may come later")


def _mask(key: str) -> str:
    return f'{key[:4]}...{key[-4:]}' if key and len(key) > 12 else '****'


def _connection_block(cfg: server.ServerConfig, keys, active: bool) -> str:
    rw, ro = keys['api_key'], keys['read_only_api_key']
    client = ("aito.Client()                       # uses the active profile" if active else
              f"aito.Client()  # after `aito profile use {cfg.profile}`, or with AITO_PROFILE={cfg.profile}")
    return f"""
  URL        {cfg.url}
  API key    {rw}   (read-write)
  Read-only  {ro}
  Profile    {cfg.profile}{' (active)' if active else ''}, in {profiles.CREDENTIALS_FILE}

  Python   import aito; client = {client}
  Shell    export AITO_URL={cfg.url} AITO_API_KEY={rw}
  curl     curl -H "x-api-key: $AITO_API_KEY" $AITO_URL/api/v2/schema
  SQL      PGPASSWORD={rw} psql -h 127.0.0.1 -p {cfg.sql_port} -U aito -d aito
"""


def _cmd_start(a) -> int:
    cfg = server.ServerConfig.for_start(a.profile, container=a.container, volume=a.volume, image=a.image,
                                        port=a.port, sql_port=a.sql_port)
    res = server.start(cfg, activate=True if a.activate else None)
    print(f"Aito is {res['state']} on {cfg.url} (ready in {res['seconds']:.1f}s, image "
          f"{cfg.image.split('@')[0].rsplit(':', 1)[-1]}, data in volume '{cfg.volume}').")
    for note in res['notes']:
        print(f"  note: {note}")
    print(_connection_block(res['config'], res['keys'], res['active']))
    print("  Stop with `aito stop`; your data and keys stay. Keys again: `aito keys`.")
    return 0


def _cmd_status(a) -> int:
    cfg = server.ServerConfig.from_profile(a.profile)
    s = server.status(cfg)
    ok = s['container'] == 'running' and s['version_http'] == 200 and s['key_accepted']
    print(f"{'up' if ok else 'NOT READY'}: profile {cfg.profile}{' (active)' if s['active'] else ''}, "
          f"{cfg.url}")
    print(f"  container  {cfg.container}: {s['container']}")
    print(f"  image      {s['image'].split('@')[0]}")
    print(f"  /version   {s['version_http'] or 'no answer'}")
    print(f"  key        {'accepted' if s['key_accepted'] else 'REFUSED or unreachable'}")
    print(f"  SQL        127.0.0.1:{cfg.sql_port}   volume {cfg.volume}")
    return 0 if ok else 1


def _cmd_logs(a) -> int:
    return server.logs(server.ServerConfig.from_profile(a.profile), follow=a.follow, tail=a.tail)


def _cmd_stop(a) -> int:
    cfg = server.ServerConfig.from_profile(a.profile)
    print(f"stopped {cfg.container}; data and keys kept in volume '{cfg.volume}'. `aito start` starts it again."
          if server.stop(cfg) else f"{cfg.container} is not running.")
    return 0


def _cmd_keys(a) -> int:
    cfg = server.ServerConfig.from_profile(a.profile)
    if a.rotate:
        res = server.rotate_keys(cfg)
        keys = res['keys']
        print("New keys are live; the old ones no longer work. Data is unchanged."
              + ("" if res['was_running'] else
                 " The server was stopped; taking the new keys started it again (`aito stop` stops it)."))
    else:
        keys = server.stored_keys(cfg.profile)
    if a.export:
        print(f"export AITO_URL={cfg.url}\nexport AITO_API_KEY={keys['api_key']}")
    else:
        print(_connection_block(cfg, keys, profiles.active_profile_name() == cfg.profile))
    return 0


def _cmd_upgrade(a) -> int:
    cfg = server.ServerConfig.from_profile(a.profile)
    res = server.upgrade(cfg, image=a.image or server.PINNED_IMAGE)
    tag = lambda i: i.split('@')[0].rsplit(':', 1)[-1]  # noqa: E731
    print(f"upgraded {cfg.container}: {tag(res['from'])} -> {tag(res['to'])}; same keys, same data.")
    return 0


def _cmd_profile(a) -> int:
    if a.action == 'use':
        if profiles.load_profile(a.name) is None:
            print(f"no profile '{a.name}' in {profiles.CREDENTIALS_FILE}", file=sys.stderr)
            return 1
        profiles.set_active_profile(a.name)
        print(f"active profile: {a.name}")
        return 0
    active = profiles.active_profile_name()
    for name, p in profiles.profiles().items():
        print(f"{'*' if name == active else ' '} {name:<12} {p.get('instance_url', '?'):<40} "
              f"key {_mask(p.get('api_key', ''))}")
    if not profiles.profiles():
        print(f"no profiles yet: `aito start` (local) or `aito configure` (an existing instance)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='aito', description='Run and manage a local Aito (needs Docker).')
    sub = parser.add_subparsers(dest='command', required=True, metavar='<command>')

    def with_profile(p):
        p.add_argument('--profile', default=server.DEFAULT_PROFILE,
                       help=f'the local server profile (default: {server.DEFAULT_PROFILE})')
        return p

    p = with_profile(sub.add_parser('start', help='start a local Aito in Docker and store its keys in a profile'))
    # No defaults here: an option not given comes from the profile, so a rerun keeps what
    # the server runs with (ServerConfig.for_start), and only then from the defaults
    p.add_argument('--port', type=int, help='HTTP port on 127.0.0.1 (default 9005)')
    p.add_argument('--sql-port', type=int,
                   help='Postgres-wire port on 127.0.0.1 (default 5432; the next free one if taken)')
    p.add_argument('--volume', help='Docker volume for the data (default aito-state; aito-<profile>-state '
                                    'for other profiles)')
    p.add_argument('--container', help='container name (default aito; aito-<profile> for other profiles)')
    p.add_argument('--image', help='run another image than the stored one (first start: the one this SDK pins)')
    p.add_argument('--activate', action='store_true',
                   help='make this the active profile even if another one is active')
    p.set_defaults(func=_cmd_start)

    with_profile(sub.add_parser('status', help='is the local Aito up, and is the stored key accepted')) \
        .set_defaults(func=_cmd_status)
    p = with_profile(sub.add_parser('logs', help="the local Aito's logs"))
    p.add_argument('-f', '--follow', action='store_true')
    p.add_argument('--tail', type=int, default=200)
    p.set_defaults(func=_cmd_logs)
    with_profile(sub.add_parser('stop', help='stop the local Aito (data and keys are kept)')) \
        .set_defaults(func=_cmd_stop)
    p = with_profile(sub.add_parser('keys', help='show the keys, or --rotate them'))
    p.add_argument('--rotate', action='store_true', help='replace both keys; the old ones stop working')
    p.add_argument('--export', action='store_true', help='print shell export lines only')
    p.set_defaults(func=_cmd_keys)
    p = with_profile(sub.add_parser('upgrade', help='move to the engine version this SDK pins'))
    p.add_argument('--image', help='a specific image instead of the pinned one')
    p.set_defaults(func=_cmd_upgrade)
    p = sub.add_parser('profile', help='list profiles, or `use NAME` to make one active')
    p.add_argument('action', nargs='?', choices=['list', 'use'], default='list')
    p.add_argument('name', nargs='?')
    p.set_defaults(func=_cmd_profile)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in RESERVED:
        print(f"aito {argv[0]}: {RESERVED_HINT}", file=sys.stderr)
        return 2
    args = build_parser().parse_args(argv)
    if getattr(args, 'action', None) == 'use' and not args.name:
        build_parser().error('profile use needs a NAME')
    try:
        return args.func(args)
    except (server.LocalServerError, profiles.NoCredentialsError) as e:
        print(f"aito {args.command}: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
