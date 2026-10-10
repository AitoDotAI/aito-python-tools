#!/usr/bin/env python3
"""One version across everything a release publishes: check it, or bump it everywhere at once

A release publishes two PyPI packages and a registry entry, and they must agree:

- ``aito/__init__.py``                     ``__version__``                (aitoai)
- ``packaging/aito-mcp/pyproject.toml``    ``version`` and the ``aitoai[mcp]==`` pin
- ``registry/server.json``                 ``version`` and the package's ``version``
- ``docs/source/changelog.rst``            a section titled with the version

    python scripts/versions.py check [--tag 1.2.1]   # all agree (and match the tag)
    python scripts/versions.py bump 1.2.1            # rewrite all; "Unreleased" becomes 1.2.1

Standard library only, so the release workflow runs it before installing anything.
"""

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INIT = 'aito/__init__.py'
MCP_PYPROJECT = 'packaging/aito-mcp/pyproject.toml'
SERVER_JSON = 'registry/server.json'
CHANGELOG = 'docs/source/changelog.rst'
VERSION_RE = re.compile(r'^\d+\.\d+\.\d+$')


def read(root: Path) -> dict:
    """every place that names the release version, and what it says"""
    init = (root / INIT).read_text()
    pyproject = (root / MCP_PYPROJECT).read_text()
    server = json.loads((root / SERVER_JSON).read_text())
    changelog = (root / CHANGELOG).read_text()
    version = re.search(r'^__version__ = "([^"]+)"', init, re.M)
    mcp_version = re.search(r'^version = "([^"]+)"', pyproject, re.M)
    pin = re.search(r'"aitoai\[mcp\]==([^"]+)"', pyproject)
    return {
        INIT: version.group(1) if version else None,
        f'{MCP_PYPROJECT} version': mcp_version.group(1) if mcp_version else None,
        f'{MCP_PYPROJECT} aitoai pin': pin.group(1) if pin else None,
        f'{SERVER_JSON} version': server.get('version'),
        f'{SERVER_JSON} package version': (server.get('packages') or [{}])[0].get('version'),
        '_changelog_sections': re.findall(r'^(\S+)\n-{3,}$', changelog, re.M),
    }


def problems(root: Path, tag: str = None) -> list:
    """what disagrees; empty when the release is consistent"""
    found = read(root)
    sections = found.pop('_changelog_sections')
    versions = set(found.values())
    out = []
    if len(versions) != 1:
        out.append('versions disagree: ' + ', '.join(f'{k} = {v}' for k, v in found.items()))
    version = next(iter(versions)) if len(versions) == 1 else found[INIT]
    if version not in sections:
        out.append(f'{CHANGELOG} has no section titled {version}')
    if tag is not None and tag != version:
        out.append(f'the tag {tag} is not the version in the files ({version})')
    return out


def bump(root: Path, new: str) -> None:
    """write ``new`` into every place, and turn the changelog's Unreleased section into it"""
    if not VERSION_RE.match(new):
        raise SystemExit(f'not a version: {new!r} (expected MAJOR.MINOR.PATCH)')
    old = read(root)[INIT]
    path = root / INIT
    path.write_text(re.sub(r'^__version__ = "[^"]+"', f'__version__ = "{new}"', path.read_text(), flags=re.M))
    path = root / MCP_PYPROJECT
    text = re.sub(r'^version = "[^"]+"', f'version = "{new}"', path.read_text(), flags=re.M)
    path.write_text(re.sub(r'"aitoai\[mcp\]==[^"]+"', f'"aitoai[mcp]=={new}"', text))
    path = root / SERVER_JSON
    server = json.loads(path.read_text())
    server['version'] = new
    for package in server.get('packages', []):
        package['version'] = new
    path.write_text(json.dumps(server, indent=2, ensure_ascii=False) + '\n')
    path = root / CHANGELOG
    text = path.read_text()
    if not re.search(r'^Unreleased\n-{3,}$', text, re.M):
        raise SystemExit(f'{CHANGELOG} has no "Unreleased" section to release as {new}')
    path.write_text(re.sub(r'^Unreleased\n-{3,}$', f'{new}\n{"-" * max(len(new), 5)}', text, count=1, flags=re.M))
    print(f'{old} -> {new} in {INIT}, {MCP_PYPROJECT}, {SERVER_JSON}, {CHANGELOG}')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    sub = parser.add_subparsers(dest='command', required=True)
    check = sub.add_parser('check', help='all versions agree (and match --tag)')
    check.add_argument('--tag', help='the release tag being published')
    sub.add_parser('bump', help='set a new version everywhere').add_argument('version')
    args = parser.parse_args()
    if args.command == 'bump':
        bump(ROOT, args.version)
        return 0
    issues = problems(ROOT, args.tag)
    for issue in issues:
        print(f'versions: {issue}', file=sys.stderr)
    if not issues:
        print(f'versions: {read(ROOT)[INIT]} everywhere')
    return 1 if issues else 0


if __name__ == '__main__':
    sys.exit(main())
