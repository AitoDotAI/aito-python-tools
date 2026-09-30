#!/usr/bin/env python3
"""The `aito start` path end to end, as a newcomer runs it, on whatever the CI runner has

    python scripts/ci/local_server_smoke.py                 # expect it to work
    python scripts/ci/local_server_smoke.py --expect-refusal "Linux containers"

aito start -> aito.Client() writes and predicts -> aito keys --rotate (the old key is
refused, the data stays, Client() follows) -> aito status -> aito stop. Each step is
timed. With --expect-refusal, `aito start` must fail cleanly: a non-zero exit, the given
phrase in its message, and no traceback. That covers hosts where Aito cannot run (Windows
containers), where the message is the product.

Uses its own config directory, so it never touches the runner user's profiles.
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


#: A candidate engine image to test instead of the pin (the workflow's `image` input).
IMAGE = os.environ.get('AITO_SMOKE_IMAGE') or None


def aito(*args, check=True):
    if IMAGE and args and args[0] == 'start':
        args = (*args, '--image', IMAGE)
    exe = shutil.which('aito')
    assert exe, "the `aito` console script is not on PATH; was the package installed?"
    t = time.monotonic()
    res = subprocess.run([exe, *args], capture_output=True, text=True)
    print(f"$ aito {' '.join(args)}  ->  exit {res.returncode} in {time.monotonic() - t:.1f}s")
    for line in (res.stdout + res.stderr).splitlines():
        print(f"    {line}")
    if check and res.returncode != 0:
        dump_logs()
        sys.exit(f"FAIL: aito {' '.join(args)} exited {res.returncode}")
    return res


def dump_logs():
    exe = shutil.which('aito')
    if exe:
        subprocess.run([exe, 'logs', '--tail', '80'])


def status_of(url, key):
    req = urllib.request.Request(f'{url}/api/v2/schema', headers={'x-api-key': key})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--expect-refusal', metavar='PHRASE')
    a = ap.parse_args()
    os.environ['XDG_CONFIG_HOME'] = tempfile.mkdtemp(prefix='aito-ci-config-')
    for var in ('AITO_URL', 'AITO_INSTANCE_URL', 'AITO_API_KEY', 'AITO_PROFILE'):
        os.environ.pop(var, None)

    if a.expect_refusal:
        res = aito('start', check=False)
        out = res.stdout + res.stderr
        assert res.returncode != 0, "aito start succeeded where it was expected to refuse"
        assert 'Traceback' not in out, "aito start crashed instead of explaining"
        assert a.expect_refusal in out, f"the refusal does not mention {a.expect_refusal!r}"
        print(f"OK: aito start refused cleanly ({a.expect_refusal!r})")
        return

    t0 = time.monotonic()
    aito('start')
    import aito as sdk  # after start: Client() reads the profile it wrote
    client = sdk.Client()
    assert client.instance_url.startswith('http://127.0.0.1:'), client.instance_url
    client.create_collection('invoices', {
        'vendor': {'type': 'String'},
        'description': {'type': 'Text', 'analyzer': 'english'},
        'gl': {'type': 'String'}})
    client.upload_entries('invoices', [
        {'vendor': 'Canon', 'description': 'printer toner', 'gl': 'Office'},
        {'vendor': 'Canon', 'description': 'paper and toner', 'gl': 'Office'},
        {'vendor': 'Finnair', 'description': 'flight to Oulu', 'gl': 'Travel'}])
    first = client.predict(from_table='invoices', where={'vendor': 'Canon'}, predict='gl').first
    print(f"first prediction: {first.value} p={first.probability:.3f}, "
          f"{time.monotonic() - t0:.1f}s after `aito start` began")
    assert first.value == 'Office', first.value

    old_url, old_key = client.instance_url, client.api_key
    aito('keys', '--rotate')
    assert status_of(old_url, old_key) == 403, "the old key still works after --rotate"
    rotated = sdk.Client()
    assert rotated.api_key != old_key
    again = rotated.predict(from_table='invoices', where={'vendor': 'Canon'}, predict='gl').first
    assert again.value == 'Office', "the data did not survive --rotate"
    print("rotate: old key refused, new key works, data kept")

    aito('status')
    aito('start')                       # idempotent on a healthy server
    aito('stop')
    res = aito('status', check=False)
    assert res.returncode == 1, "status should report NOT READY after stop"
    for word in ('serve', 'run', 'up'):
        res = aito(word, check=False)
        assert res.returncode == 2 and 'aito start' in res.stderr, word
    print(f"OK: the whole path in {time.monotonic() - t0:.1f}s")


if __name__ == '__main__':
    main()
