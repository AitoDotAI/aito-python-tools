"""The MCP server end to end: ``aito-mcp`` as a stdio subprocess against a live Aito

Skipped unless ``AITO_MCP_SMOKE=1`` and ``AITO_INSTANCE_URL`` / ``AITO_API_KEY`` (read-
write) are set. Its own switch, not just the instance variables the other live
suites use: this suite is meant for a LOCAL instance (``aito start``), and CI sets
the instance variables for a shared one. It creates two uniquely named tables,
calls every tool through the protocol, and drops the tables again.
"""

import asyncio
import json
import os
import sys
import unittest
from uuid import uuid4

from aito.v2 import AitoClientV2
from tests.cases import BaseTestCase

try:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
except ImportError:
    ClientSession = None

SUFFIX = uuid4().hex[:8]
PRODUCTS, LINES = f'mcp_smoke_products_{SUFFIX}', f'mcp_smoke_lines_{SUFFIX}'
CATALOGUE = [
    ('SKU-TONER', 'black toner cartridge for laser printer', 'office'),
    ('SKU-PAPER', 'copy paper a4 500 sheets', 'office'),
    ('SKU-DIESEL', 'diesel fuel litre', 'fuel'),
    ('SKU-LUNCH', 'staff lunch buffet', 'catering'),
]
VENDORS = [('Paperi Oy', 'SKU-PAPER', '4010'), ('Toner Tukku', 'SKU-TONER', '4010'),
           ('Neste Oyj', 'SKU-DIESEL', '6200'), ('Fazer Food Services', 'SKU-LUNCH', '7300')]


def _rows(count=200):
    names = {sku: name for sku, name, _ in CATALOGUE}
    return [{'line_id': f'l{i}', 'vendor': VENDORS[i % 4][0], 'description': names[VENDORS[i % 4][1]],
             'amount': 20 + i % 90, 'gl_account': VENDORS[i % 4][2], 'product': VENDORS[i % 4][1]}
            for i in range(count)]


def _live():
    return (os.getenv('AITO_MCP_SMOKE') == '1' and os.getenv('AITO_INSTANCE_URL')
            and os.getenv('AITO_API_KEY'))


@unittest.skipUnless(ClientSession is not None and _live(),
                     'AITO_MCP_SMOKE=1, AITO_INSTANCE_URL and a read-write AITO_API_KEY are required')
class TestMcpLiveSmoke(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.client = AitoClientV2(os.environ['AITO_INSTANCE_URL'], os.environ['AITO_API_KEY'],
                                   check_credentials=False)

    def tearDown(self):
        for table in (LINES, PRODUCTS):
            try:
                self.client.request('DELETE', f'/schema/{table}')
            except Exception:  # noqa: BLE001 - best-effort cleanup of this run's own tables
                pass
        super().tearDown()

    def test_every_tool_through_the_protocol(self):
        asyncio.run(self._session())

    async def _session(self):
        env = {**os.environ, 'AITO_MCP_ALLOW_WRITES': '1'}
        params = StdioServerParameters(command=sys.executable, args=['-m', 'aito.mcp.server'], env=env)
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                init = await session.initialize()
                self.assertIn('separate instance or collection per customer', init.instructions)
                tools = {t.name for t in (await session.list_tools()).tools}
                self.assertEqual(len(tools), 10)

                async def call(tool, **arguments):
                    result = await session.call_tool(tool, arguments)
                    error = getattr(result, 'is_error', getattr(result, 'isError', False))
                    return error, result.content[0].text

                async def ok(tool, **arguments):
                    error, text = await call(tool, **arguments)
                    self.assertFalse(error, f'{tool}: {text}')
                    return json.loads(text)

                await ok('put_schema', table=PRODUCTS, schema={'type': 'collection', 'columns': {
                    'sku': {'type': 'String'}, 'name': {'type': 'Text', 'analyzer': 'english'},
                    'category': {'type': 'String'}}})
                await ok('put_schema', table=LINES, schema={'type': 'collection', 'columns': {
                    'line_id': {'type': 'String'}, 'vendor': {'type': 'String'},
                    'description': {'type': 'Text', 'analyzer': 'english'}, 'amount': {'type': 'Int'},
                    'gl_account': {'type': 'String'},
                    'product': {'type': 'String', 'link': f'{PRODUCTS}.sku'}}})
                await ok('upload_rows', table=PRODUCTS,
                         rows=[{'sku': s, 'name': n, 'category': c} for s, n, c in CATALOGUE])
                self.assertEqual((await ok('upload_rows', table=LINES, rows=_rows()))['uploaded'], 200)

                schema = await ok('get_schema', table=LINES)
                self.assertEqual(schema['columns']['product']['link'], f'{PRODUCTS}.sku')

                predicted = await ok('predict', query={
                    'from': LINES, 'where': {'vendor': 'Neste Oyj'}, 'predict': 'gl_account', 'limit': 2})
                top = predicted['hits'][0]
                self.assertEqual(top['$value'], '6200')
                self.assertGreater(top['$p'], 0.5)
                self.assertIn('base_p', top['$why'])
                self.assertTrue(top['$why']['factors'], top)

                recommended = await ok('recommend', query={
                    'from': LINES, 'where': {'vendor': 'Paperi Oy'}, 'recommend': 'product',
                    'goal': {'gl_account': '4010'}, 'limit': 2})
                self.assertIn('$p', recommended['hits'][0])

                related = await ok('relate', query={
                    'from': LINES, 'where': {'gl_account': '7300'}, 'relate': 'vendor'})
                self.assertTrue(related['hits'])

                matched = await ok('match', query={
                    'from': LINES, 'where': {'description': 'diesel fuel'}, 'match': 'product', 'limit': 2})
                self.assertEqual(matched['hits'][0]['$value'], 'SKU-DIESEL')
                self.assertTrue(matched['hits'][0]['$why']['factors'])

                found = await ok('search', query={'from': LINES, 'where': {'vendor': 'Neste Oyj'}, 'limit': 3})
                self.assertEqual(found['total'], 50)
                queried = await ok('query', query={'from': PRODUCTS, 'select': ['sku'], 'limit': 10})
                self.assertEqual(queried['total'], 4)

                evaluation = await ok('evaluate', query={
                    'test': {'$index': {'$mod': [5, 0]}},  # spans all four vendors
                    'evaluate': {'from': LINES, 'where': {'vendor': {'$get': 'vendor'}},
                                 'predict': 'gl_account'}})
                self.assertGreater(evaluation.get('accuracy', 0), 0.9, evaluation)

                error, text = await call('predict', query={'from': LINES, 'predict': 'no_such_field'})
                self.assertTrue(error)
                self.assertIn('no_such_field', text)
