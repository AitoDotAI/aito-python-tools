from aito.schema import AitoDatabaseSchema, AitoTableSchema, AitoColumnTypeSchema, AitoStringType
from tests.cases import CompareTestCase
import aito.client.requests as aito_requests
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
                None
        ),
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
                    table_name='table_name', column_name='column_name', schema={'type': 'String', 'nullable': False}
                ),
                None
        ),
        ('delete_database_schema', 'DELETE', '/api/v1/schema', {}, aito_requests.DeleteDatabaseSchemaRequest(), None),
        (
                'delete_table_schema', 'DELETE', '/api/v1/schema/table_name', {},
                aito_requests.DeleteTableSchemaRequest(table_name='table_name'), None),
        (
                'delete_column_schema', 'DELETE', '/api/v1/schema/table_name/column_name', {},
                aito_requests.DeleteColumnSchemaRequest(table_name='table_name', column_name='column_name'), None
        ),
        (
                'init_file_upload', 'POST', '/api/v1/data/table_name/file', {},
                aito_requests.InitiateFileUploadRequest(table_name='table_name'), None
        ),
        (
                'trigger_file_processing', 'POST', '/api/v1/data/table_name/file/00000000-0000-0000-0000-000000000000', {},
                aito_requests.TriggerFileProcessingRequest(table_name='table_name', session_id='00000000-0000-0000-0000-000000000000'), None
        ),
        (
                'get_file_processing_status', 'GET', '/api/v1/data/table_name/file/00000000-0000-0000-0000-000000000000', {},
                aito_requests.GetFileProcessingRequest(table_name='table_name', session_id='00000000-0000-0000-0000-000000000000'), None
        ),
        (
                'create_job', 'POST', '/api/v1/jobs/_search', {},
                aito_requests.CreateJobRequest(endpoint='/api/v1/jobs/_search', query={}), None
        ),
        (
                'get_job_status', 'GET', '/api/v1/jobs/00000000-0000-0000-0000-000000000000', {},
                aito_requests.GetJobStatusRequest(job_id='00000000-0000-0000-0000-000000000000'), None
        ),
        (
                'get_job_result', 'GET', '/api/v1/jobs/00000000-0000-0000-0000-000000000000/result', {},
                aito_requests.GetJobResultRequest(job_id='00000000-0000-0000-0000-000000000000'), None
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
