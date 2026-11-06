#!/usr/bin/env python3
"""Load this fixture into a local Aito the live tests then query

    AITO_GROCERY_DEMO_INSTANCE_URL=http://localhost:9005 AITO_GROCERY_DEMO_API_KEY=... \\
        python tests/fixtures/grocery/seed.py

It replaces the schema, so it refuses any host but localhost: never a shared instance.
Standard library only, so CI runs it before installing anything.
"""

import gzip
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

HERE = Path(__file__).resolve().parent
TABLES = ('users', 'products', 'visits', 'contexts', 'impressions')
BATCH = 5000
LOCAL = ('localhost', '127.0.0.1', '::1')


def call(url, key, method, path, body):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url + path, data=data, method=method,
                                 headers={'x-api-key': key, 'content-type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        sys.exit(f'{method} {path}: {e.code} {e.read()[:300].decode(errors="replace")}')


def main():
    url = os.environ['AITO_GROCERY_DEMO_INSTANCE_URL'].rstrip('/')
    key = os.environ['AITO_GROCERY_DEMO_API_KEY']
    if urlparse(url).hostname not in LOCAL:
        sys.exit(f'refusing to replace the schema of {urlparse(url).hostname}: the fixture loads into a local Aito only')
    call(url, key, 'DELETE', '/api/v1/schema', None)   # every table and its rows: a clean start
    call(url, key, 'PUT', '/api/v1/schema', {'schema': json.loads((HERE / 'schema.json').read_text())})
    for table in TABLES:
        with gzip.open(HERE / f'{table}.ndjson.gz', 'rt') as f:
            rows = [json.loads(line) for line in f]
        for i in range(0, len(rows), BATCH):
            call(url, key, 'POST', f'/api/v1/data/{table}/batch', rows[i:i + BATCH])
        count = call(url, key, 'POST', '/api/v1/_search', {'from': table, 'limit': 0})['total']
        if count != len(rows):
            sys.exit(f'{table}: loaded {count} rows, the fixture has {len(rows)}')
        print(f'{table}: {count} rows')


if __name__ == '__main__':
    main()
