#!/usr/bin/env python3
"""Pin the engine image `aito start` runs to a published tag, by digest

    python scripts/pin-engine.py v2.11.2            # resolve, check it is multi-arch, rewrite the pin
    python scripts/pin-engine.py v2.11.2 --allow-single-arch
    python scripts/pin-engine.py --check-multiarch  # exit 1 if the current pin is single-arch
    python scripts/pin-engine.py --show             # the current pin and its platforms

The pin is ``PINNED_IMAGE`` in aito/local/server.py: ``<repo>:<tag>@sha256:<digest>``. For a
multi-arch release the digest is the INDEX's, so Docker picks the platform (a per-arch digest
would pin one architecture). A tag can be re-pointed; the digest cannot. Anonymous ghcr pull,
standard library only.
"""
import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO = 'aitohq/aito'
SERVER_PY = Path(__file__).resolve().parents[1] / 'aito' / 'local' / 'server.py'
PIN_RE = re.compile(r"PINNED_IMAGE = \('ghcr\.io/aitohq/aito:(?P<tag>[^'@]+)'\n\s+'@(?P<digest>sha256:[0-9a-f]{64})'\)")
ACCEPT = ', '.join([
    'application/vnd.oci.image.index.v1+json',
    'application/vnd.docker.distribution.manifest.list.v2+json',
    'application/vnd.oci.image.manifest.v1+json',
    'application/vnd.docker.distribution.manifest.v2+json',
])


def _get(url: str, token: str, accept: str = ACCEPT):
    req = urllib.request.Request(url, headers={'Authorization': f'Bearer {token}', 'Accept': accept})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.headers.get('Docker-Content-Digest'), json.load(r)


def resolve(ref: str):
    """(digest, platforms) for a tag or digest reference on ghcr"""
    with urllib.request.urlopen(f'https://ghcr.io/token?scope=repository:{REPO}:pull', timeout=30) as r:
        token = json.load(r)['token']
    digest, manifest = _get(f'https://ghcr.io/v2/{REPO}/manifests/{ref}', token)
    if 'manifests' in manifest:   # an index / manifest list
        platforms = sorted({f"{m['platform']['os']}/{m['platform']['architecture']}"
                            for m in manifest['manifests']
                            if m.get('platform', {}).get('os') not in (None, 'unknown')})
    else:                          # a single manifest: its config names the one platform
        _, config = _get(f"https://ghcr.io/v2/{REPO}/blobs/{manifest['config']['digest']}", token,
                         accept='application/octet-stream')
        platforms = [f"{config.get('os')}/{config.get('architecture')}"]
    return digest, platforms


def current_pin():
    m = PIN_RE.search(SERVER_PY.read_text())
    if not m:
        sys.exit(f'no PINNED_IMAGE of the expected shape in {SERVER_PY}')
    return m.group('tag'), m.group('digest')


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('tag', nargs='?')
    ap.add_argument('--allow-single-arch', action='store_true')
    ap.add_argument('--check-multiarch', nargs='?', const='', metavar='REF',
                    help='exit 1 unless REF (default: the current pin) is amd64+arm64')
    ap.add_argument('--show', action='store_true')
    a = ap.parse_args()

    if a.show or a.check_multiarch is not None:
        if a.check_multiarch:   # a candidate: repo:tag, repo:tag@digest, a tag or a digest
            ref = a.check_multiarch.split('@')[-1] if '@' in a.check_multiarch else a.check_multiarch.split(':')[-1]
            tag, digest = a.check_multiarch, ref
            shown = a.check_multiarch
        else:
            tag, digest = current_pin()
            shown = f'{tag}@{digest}'
        _, platforms = resolve(digest)
        multi = {'linux/amd64', 'linux/arm64'} <= set(platforms)
        print(f'{shown}: {", ".join(platforms)}{"" if multi else "  (single-arch)"}')
        return 0 if (multi or a.show) else 1

    if not a.tag:
        ap.error('give a tag (e.g. v2.11.2), --show or --check-multiarch')
    try:
        digest, platforms = resolve(a.tag)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            sys.exit(f'{a.tag} is not published on ghcr.io/{REPO} (yet)')
        raise
    if not digest:
        sys.exit(f'ghcr gave no digest for {a.tag}')
    if not {'linux/amd64', 'linux/arm64'} <= set(platforms) and not a.allow_single_arch:
        sys.exit(f'{a.tag} is {", ".join(platforms)} only: Apple Silicon and arm64 Linux would run it under '
                 f'emulation. Pin a multi-arch release, or pass --allow-single-arch.')
    source = SERVER_PY.read_text()
    old_tag, old_digest = current_pin()
    new = PIN_RE.sub(f"PINNED_IMAGE = ('ghcr.io/aitohq/aito:{a.tag}'\n                '@{digest}')", source)
    SERVER_PY.write_text(new)
    print(f'pinned {a.tag}@{digest} ({", ".join(platforms)}); was {old_tag}@{old_digest[:19]}...')
    return 0


if __name__ == '__main__':
    sys.exit(main())
