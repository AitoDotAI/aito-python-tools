#!/usr/bin/env python3
"""A worked example of the Aito v2 Python client, end to end.

Creates a collection, loads it, asks it questions, and drops it again. Every
call here runs against a real instance — nothing is mocked, and the script
leaves the database as it found it.

Run it::

    export AITO_INSTANCE_URL=https://shared.aito.ai/db/your-database
    export AITO_API_KEY=<a read-write key>
    python examples/v2_quickstart.py

The data is an invoice ledger: four vendors, each of which is nearly always
booked to the same general-ledger code. That relationship is not written down
anywhere — the point is that Aito learns it from the rows and can then predict
the GL code for an invoice it has not seen, and explain why.
"""

import os
import sys

from aito.client.v2 import AitoClientV2, AitoV2Error

COLLECTION = 'v2_quickstart_invoices'

#: vendor -> (what they invoice for, the GL code it is booked to)
LEDGER = [
    ('Elenia Oy', 'electricity network transfer for the Tampere site', '6110'),
    ('Neste Oyj', 'diesel fuel purchase for the delivery fleet', '6200'),
    ('Fazer Food Services', 'staff lunch catering, monthly invoice', '7300'),
    ('Telia Finland', 'mobile subscriptions and data, monthly', '6400'),
]


def build_entries(count=400):
    """A deterministic ledger: vendor implies GL code, amounts vary."""
    entries = []
    for i in range(count):
        vendor, description, gl_code = LEDGER[i % len(LEDGER)]
        entries.append({
            'vendor': vendor,
            'description': description,
            'amount': round(50 + (i * 7 % 850) + 0.5, 2),
            'gl_code': gl_code,
        })
    return entries


def step(title):
    print(f'\n=== {title} ===')


def main():
    instance_url = os.getenv('AITO_INSTANCE_URL')
    api_key = os.getenv('AITO_API_KEY')
    if not instance_url or not api_key:
        print('Set AITO_INSTANCE_URL and a read-write AITO_API_KEY first.', file=sys.stderr)
        return 1

    # `on_warning='raise'` turns a query the server had to broaden into a hard
    # failure. In an example that is a way to prove the queries below are
    # actually the queries that ran; in a multi-tenant app it is how you find
    # out that a filter was dropped before your users do.
    client = AitoClientV2(instance_url, api_key, on_warning='raise')
    print(f'connected: {client}')
    print(f"instance:  {client.get_version()['version']}")

    step('1. create the collection')
    # `drop if exists` — a 404 here is the ordinary case, not a failure, and
    # `is_not_found` is how you tell it from a real error without matching
    # strings in the message.
    try:
        client.delete_collection(COLLECTION)
        print(f'dropped the previous {COLLECTION}')
    except AitoV2Error as err:
        if not err.is_not_found:
            raise
    client.create_collection(COLLECTION, {
        'vendor': {'type': 'String'},
        'description': {'type': 'Text', 'analyzer': 'english'},
        'amount': {'type': 'Decimal'},
        'gl_code': {'type': 'String'},
    })
    print(f'created collection {COLLECTION}')

    step('2. load the data')
    entries = build_entries()
    inserted = client.upload_entries(COLLECTION, entries, batch_size=200)
    print(f'inserted {inserted} rows')
    # Merges the per-insert segments. Skip it and predict's posteriors come back
    # flatter, and dependent on how many batches you happened to use.
    client.optimize(COLLECTION)
    print('optimized')

    step('3. read rows back')
    rows = client.search(from_table=COLLECTION, where={'vendor': 'Elenia Oy'},
                         select=['vendor', 'amount', 'gl_code'], limit=3)
    print(f'{rows.total} Elenia invoices; first {len(rows)}:')
    for hit in rows:
        print(f"  {hit['vendor']:<22} {hit['amount']:>8}  -> {hit['gl_code']}")

    step('4. predict the GL code for an invoice, from the vendor alone')
    prediction = client.predict(from_table=COLLECTION,
                                where={'vendor': 'Neste Oyj'},
                                predict='gl_code', limit=3)
    for hit in prediction:
        print(f'  {hit.value}  p={hit.probability:.4f}')
    top = prediction.first
    print(f'-> book it to {top.value} (confidence {top.probability:.1%})')

    step('5. predict from free text instead, and ask why')
    # No vendor given: only the description, matched through the analyzed Text
    # column. This is the query a real integration makes — you have the invoice
    # text, not a clean vendor key.
    explained = client.predict(from_table=COLLECTION,
                               where={'description': 'catering for the staff lunch'},
                               predict='gl_code', why=True, limit=1)
    hit = explained.first
    print(f'  text -> {hit.value}  p={hit.probability:.4f}')
    print(f"  why: {str(hit.why)[:160]}...")

    step('6. which vendors explain a GL code?')
    relations = client.relate(from_table=COLLECTION, where={'gl_code': '6110'},
                              relate='vendor', limit=3)
    for hit in relations:
        related = hit['related']
        print(f"  {related}  lift={hit['lift']:.2f}  "
              f"f={hit['fs']['fOnCondition']:.0f}/{hit['fs']['fCondition']:.0f}")

    step('7. estimate a numeric field')
    estimate = client.estimate(from_table=COLLECTION,
                               where={'vendor': 'Fazer Food Services'}, estimate='amount')
    print(f'  expected amount for a Fazer invoice: {estimate.value:.2f}')

    step('8. aggregate')
    totals = client.aggregate(from_table=COLLECTION, aggregate=['amount.$sum', 'amount.$mean'])
    print(f"  total {totals['amount.$sum']:.2f} over {totals['amount.$mean.samples']} invoices "
          f"(mean {totals['amount.$mean']:.2f})")

    step('9. how good is the prediction, really?')
    # Hold out every tenth row, predict it from the vendor, compare to truth.
    # `baseAccuracy` is what always guessing the most common GL code would get —
    # accuracy only means something next to it.
    evaluation = client.evaluate({
        'test': {'$index': {'$mod': [10, 0]}},
        'evaluate': {
            'from': COLLECTION,
            'where': {'vendor': {'$get': 'vendor'}},
            'predict': 'gl_code',
        },
    })
    print(f'  accuracy      {evaluation.accuracy:.3f}')
    print(f'  base accuracy {evaluation.base_accuracy:.3f}  (always guess the most common)')
    print(f'  tested on {evaluation.test_sample_count} rows, '
          f'trained on {evaluation.train_sample_count}')

    step('10. errors are typed, not strings')
    try:
        client.query({'from': 'a_collection_that_does_not_exist', 'limit': 1})
    except AitoV2Error as err:
        print(f'  code={err.code!r} status={err.status_code} is_not_found={err.is_not_found}')

    step('clean up')
    client.delete_collection(COLLECTION)
    print(f'dropped {COLLECTION}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
