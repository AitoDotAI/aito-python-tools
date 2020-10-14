from aito.schema import AitoDatabaseSchema, AitoTableSchema, AitoColumnTypeSchema, AitoStringType
from tests.cases import CompareTestCase
import aito.client_request as aito_requests
from parameterized import parameterized


class TestClientRequest(CompareTestCase):
    @parameterized.expand([
        ('search', 'POST', '/api/v1/_search', {}, aito_requests.SearchRequest({}), None),
        ('predict', 'POST', '/api/v1/_predict', {}, aito_requests.PredictRequest({}), None),
        ('recommend', 'POST', '/api/v1/_recommend', {}, aito_requests.RecommendRequest({}), None),
        ('evaluate', 'POST', '/api/v1/_evaluate', {}, aito_requests.EvaluateRequest({}), None),
        ('similarity', 'POST', '/api/v1/_similarity', {}, aito_requests.SimilarityRequest({}), None),
        ('relate', 'POST', '/api/v1/_relate', {}, aito_requests.RelateRequest({}), None),
        ('query', 'POST', '/api/v1/_query', {}, aito_requests.GenericQueryRequest({}), None),
        ('get_database_schema', 'GET', '/api/v1/schema', {}, aito_requests.GetDatabaseSchemaRequest(), None),
        (
                'get_table_schema', 'GET', '/api/v1/schema/table_name', {},
                aito_requests.GetTableSchemaRequest(table_name='table_name'), None),
        (
                'get_column_schema', 'GET', '/api/v1/schema/table_name/column_name', {},
                aito_requests.GetColumnSchemaRequest(table_name='table_name', column_name='column_name'), None),
        (
                'create_database_schema', 'PUT', '/api/v1/schema',
                {'schema': {'tbl': {'type': 'table', 'columns': {'col1': {'type': 'String', 'nullable': False}}}}},
                aito_requests.CreateDatabaseSchemaRequest(schema=AitoDatabaseSchema(
                    tables={'tbl': AitoTableSchema(
                        columns={'col1': AitoColumnTypeSchema(data_type=AitoStringType())}
                    )}
                )),
                None),
        (
                'create_table_schema', 'PUT', '/api/v1/schema/table_name',
                {'type': 'table', 'columns': {'col1': {'type': 'String', 'nullable': False}}},
                aito_requests.CreateTableSchemaRequest(
                    table_name='table_name',
                    schema=AitoTableSchema(columns={'col1': AitoColumnTypeSchema(data_type=AitoStringType())})
                ),
                None
        ),
        (
                'create_column_schema', 'PUT', '/api/v1/schema/table_name/column_name',
                {'type': 'String', 'nullable': False},
                aito_requests.CreateColumnSchemaRequest(
                    table_name='table_name', column_name='column_name',
                    schema=AitoColumnTypeSchema(data_type=AitoStringType())
                ),
                None
        ),
        ('erroneous_method', 'PATCH', '/api/v1/schema', {}, None, ValueError),
        ('erroneous_endpoint', 'GET', 'api/v1/schema', {}, None, ValueError),
    ])
    def test_make_request(self, _, method, endpoint, query, expected_request_instance, error):
        if error:
            with self.assertRaises(error):
                aito_requests.AitoRequest.make_request(method=method, endpoint=endpoint, query=query)
        else:
            req = aito_requests.AitoRequest.make_request(method=method, endpoint=endpoint, query=query)
            self.assertEqual(req, expected_request_instance)

    def test_base_request_erroneous_method(self):
        with self.assertRaises(ValueError):
            aito_requests.BaseRequest('PATCH', '/api/v1/schema')

    def test_base_request_erroneous_endpoint(self):
        with self.assertRaises(ValueError):
            aito_requests.BaseRequest('GET', 'api/v1/schema')
