"""A v2 collection can be created with a primary key, and rows upserted into it

``create_collection(name, columns, primary_key='id')`` sends the engine's top-level
``primaryKey`` (a column, or a list of them for a composite key). A collection with a key
refuses a row whose key is already there, unless the batch is sent with
``upload_entries(..., on_conflict='update')`` (the row replaces the stored one) or
``on_conflict='ignore'`` (the stored row stays). Either way the same batch sent twice leaves
the collection as one send does. Without a key the engine refuses both with a 400, so
nothing is ever appended twice by them. Offline: a recording session plays the server; the
live suite (tests/sdk/test_v2_live.py) runs the same against an engine.
"""

from tests.cases import BaseTestCase
from tests.sdk.test_v2_client import FakeResponse, make_client


def inserted(count):
    return FakeResponse(200, {'status': 'inserted', 'table': 'invoices', 'count': count})


class TestCreateCollectionWithAPrimaryKey(BaseTestCase):
    def test_a_key_column_is_sent_as_the_top_level_primary_key(self):
        client = make_client()
        client.create_collection('invoices', {'id': {'type': 'String'}}, primary_key='id')
        self.assertEqual(client._session.last['json'],
                         {'type': 'collection', 'columns': {'id': {'type': 'String'}}, 'primaryKey': 'id'})

    def test_a_composite_key_is_sent_as_a_list(self):
        client = make_client()
        client.create_collection('lines', {'invoice': {'type': 'String'}, 'line': {'type': 'Int'}},
                                 primary_key=['invoice', 'line'])
        self.assertEqual(client._session.last['json']['primaryKey'], ['invoice', 'line'])

    def test_without_a_key_the_body_is_as_before(self):
        client = make_client()
        client.create_collection('invoices', {'id': {'type': 'String'}})
        self.assertEqual(client._session.last['json'], {'type': 'collection', 'columns': {'id': {'type': 'String'}}})

    def test_a_key_column_must_be_declared(self):
        # the engine refuses it too (schema.create_failed); said before anything is sent
        client = make_client()
        with self.assertRaises(ValueError) as raised:
            client.create_collection('invoices', {'id': {'type': 'String'}}, primary_key='number')
        self.assertIn("'number'", str(raised.exception))
        self.assertEqual(client._session.calls, [])


class TestUploadEntriesOnConflict(BaseTestCase):
    def test_on_conflict_is_sent_with_every_batch(self):
        client = make_client([inserted(2), inserted(1)])
        client.upload_entries('invoices', [{'id': 'a'}, {'id': 'b'}, {'id': 'c'}], batch_size=2,
                              on_conflict='update')
        self.assertEqual([c['url'].split('/api/v2')[1] for c in client._session.calls],
                         ['/data/invoices/batch?on_conflict=update'] * 2)

    def test_ignore_and_error_are_sent_as_given(self):
        for value in ('ignore', 'error'):
            with self.subTest(value):
                client = make_client([inserted(1)])
                client.upload_entries('invoices', [{'id': 'a'}], on_conflict=value)
                self.assertTrue(client._session.last['url'].endswith(f'/data/invoices/batch?on_conflict={value}'))

    def test_without_it_the_request_is_as_before(self):
        client = make_client([inserted(1)])
        client.upload_entries('invoices', [{'id': 'a'}])
        self.assertTrue(client._session.last['url'].endswith('/data/invoices/batch'))

    def test_an_unknown_value_is_refused_before_anything_is_sent(self):
        client = make_client()
        with self.assertRaises(ValueError) as raised:
            client.upload_entries('invoices', [{'id': 'a'}], on_conflict='replace')
        self.assertIn("'error', 'update' or 'ignore'", str(raised.exception))
        self.assertEqual(client._session.calls, [])
