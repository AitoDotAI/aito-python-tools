#!/usr/bin/env python3
"""Rebuild this fixture from the grocery demo data in AitoDotAI/aito-demo (src/data, Apache-2.0)

    python tests/fixtures/grocery/make.py ~/src/aito-demo/src/data

All users and products; one context in ten (every one of veronica's, whom the tests query),
with the visits and impressions that belong to them. That keeps every table under the 10,000
rows the engine's free tier holds, which is what CI runs. Deterministic: the same source
gives the same files.
"""

import gzip
import json
import sys
from pathlib import Path

TABLES = ('users', 'products', 'visits', 'contexts', 'impressions')
HERE = Path(__file__).resolve().parent


def main(src: Path):
    schema = json.loads((src / 'schema.json').read_text())
    schema = schema.get('schema', schema)
    contexts = [c for i, c in enumerate(json.loads((src / 'contexts.json').read_text()))
                if i % 10 == 0 or c['user'] == 'veronica']
    kept = {c['id'] for c in contexts}
    visits = {c['visit'] for c in contexts}
    rows = {
        'users': json.loads((src / 'users.json').read_text()),
        'products': json.loads((src / 'products.json').read_text()),
        'visits': [v for v in json.loads((src / 'visits.json').read_text()) if v['id'] in visits],
        'contexts': contexts,
        'impressions': [r for r in map(json.loads, (src / 'impressions.ndjson').open())
                        if r['context'] in kept],
    }
    assert all(len(r) < 10000 for r in rows.values()), {t: len(r) for t, r in rows.items()}
    (HERE / 'schema.json').write_text(json.dumps({t: schema[t] for t in TABLES}, indent=2, sort_keys=True) + '\n')
    for table in TABLES:
        with gzip.GzipFile(HERE / f'{table}.ndjson.gz', 'wb', mtime=0) as out:
            out.write(''.join(json.dumps(r, sort_keys=True) + '\n' for r in rows[table]).encode())
        print(f'{table}: {len(rows[table])} rows')


if __name__ == '__main__':
    main(Path(sys.argv[1]))
