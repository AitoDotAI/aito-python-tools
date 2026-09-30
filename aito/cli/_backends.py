"""What each CLI command does against the v1 API and against the v2 API.

The commands in ``database_sub_command`` say *what* to do (create a table, upload
a file, send a predict query); a backend says *how*, for one API version. The v1
backend is the CLI as it was before 1.1, calling the ``aito.v1.api`` helpers. The
v2 backend maps the same commands onto ``aito.v2.Client``: tables are created as
v2 collections, files are uploaded as JSON rows, and a query command posts its
body unchanged to the matching ``/api/v2/_<endpoint>``.

Two v1 commands have no v2 equivalent, and the v2 backend refuses them rather
than approximating: ``rename-table`` (the engine has no ``/schema/_rename`` on
v2) and ``similarity`` (no ``_similarity`` endpoint). ``--use-job`` is v1 only.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from aito.schema import AitoTableSchema
from aito.utils.data_frame_handler import DataFrameHandler


class NotSupportedOnV2(Exception):
    """the command, or an option of it, has no v2 equivalent"""


def _schema_dict(schema: Union[AitoTableSchema, Dict]) -> Dict:
    return schema.to_json_serializable() if isinstance(schema, AitoTableSchema) else schema


def _columns_of(table_schema: Union[AitoTableSchema, Dict]) -> Dict:
    """the columns of a table schema, whether v1 (``type: table``) or v2 (``type: collection``)

    A v1 table schema's columns are valid v2 collection columns as they are
    (measured on engine 2.11.1), so a v1 schema file creates a v2 collection
    without editing.
    """
    schema = _schema_dict(table_schema)
    if 'columns' not in schema:
        raise ValueError("a table schema must have 'columns'")
    return {name: _v2_column(name, column) for name, column in schema['columns'].items()}


def _v2_column(name: str, column: Dict) -> Dict:
    """a v1 column definition as v2 accepts it

    v2 takes a text analyzer only as an alias ('english', 'standard', ...). The
    v1 schema inference writes a language analyzer as an object, which v2
    refuses, so an object with no custom stop or key words becomes its language's
    alias. One with custom words has no v2 spelling and is refused by name.
    """
    analyzer = column.get('analyzer')
    if not (isinstance(analyzer, dict) and analyzer.get('type') == 'language'):
        return column
    if analyzer.get('customStopWords') or analyzer.get('customKeyWords'):
        raise NotSupportedOnV2(
            f"column '{name}': a language analyzer with custom stop or key words has no v2 "
            f"equivalent (v2 takes an analyzer alias such as '{analyzer.get('language')}')")
    return {**column, 'analyzer': analyzer['language']}


def _df_to_rows(df) -> List[Dict]:
    # through pandas' own JSON writer, so NaN becomes null and timestamps and
    # numpy scalars become JSON values the engine accepts
    return json.loads(df.to_json(orient='records', date_format='iso'))


class V1Backend:
    """the CLI as it was before 1.1: every command through ``aito.v1.api``"""

    api_version = 'v1'

    def __init__(self, client):
        from aito.v1 import api
        self.client = client
        self._api = api

    def create_database(self, schema: Dict):
        self._api.create_database(client=self.client, schema=schema)

    def create_table(self, table_name: str, schema: Union[AitoTableSchema, Dict]):
        self._api.create_table(client=self.client, table_name=table_name, schema=schema)

    def table_schema_json(self, table_name: str) -> str:
        return self._api.get_table_schema(self.client, table_name).to_json_string(indent=2)

    def database_schema_json(self) -> str:
        return self._api.get_database_schema(self.client).to_json_string(indent=2)

    def show_tables(self) -> List[str]:
        return sorted(self._api.get_existing_tables(self.client))

    def delete_table(self, table_name: str):
        self._api.delete_table(self.client, table_name)

    def delete_database(self):
        self._api.delete_database(self.client)

    def copy_table(self, table_name: str, copy_table_name: str, replace: bool):
        self._api.copy_table(self.client, table_name, copy_table_name, replace)

    def rename_table(self, old_name: str, new_name: str, replace: bool):
        self._api.rename_table(self.client, old_name, new_name, replace)

    def upload_entries(self, table_name: str, entries: List[Dict]):
        self._api.upload_entries(self.client, table_name=table_name, entries=entries)

    def optimize_table(self, table_name: str):
        self._api.optimize_table(self.client, table_name=table_name)

    def upload_file(self, table_name: str, in_file_path: Path, in_format: str):
        import tempfile
        from os import unlink
        converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=False)
        DataFrameHandler().convert_file(
            read_input=in_file_path, write_output=converted_tmp_file.name, in_format=in_format,
            out_format='ndjson', convert_options={'compression': 'gzip'},
            use_table_schema=self._api.get_table_schema(self.client, table_name))
        converted_tmp_file.close()
        with open(converted_tmp_file.name, 'rb') as in_f:
            self._api.upload_binary_file(client=self.client, table_name=table_name, binary_file=in_f)
        unlink(converted_tmp_file.name)

    def upload_data_frame(self, table_name: str, df, create_with_schema: Optional[AitoTableSchema] = None):
        import tempfile
        from os import unlink
        converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=False)
        DataFrameHandler().df_to_format(df, 'ndjson', converted_tmp_file.name, {'compression': 'gzip'})
        converted_tmp_file.close()
        if create_with_schema is not None:
            self._api.create_table(self.client, table_name, create_with_schema)
        with open(converted_tmp_file.name, 'rb') as in_f:
            self._api.upload_binary_file(client=self.client, table_name=table_name, binary_file=in_f)
        unlink(converted_tmp_file.name)

    def quick_add_table(self, input_file: Path, input_format: Optional[str], table_name: Optional[str]):
        self._api.quick_add_table(
            client=self.client, input_file=input_file, input_format=input_format, table_name=table_name)

    def quick_predict_and_evaluate(self, from_table: str, predicting_field: str) -> Tuple[Dict, Dict]:
        return self._api.quick_predict_and_evaluate(
            client=self.client, from_table=from_table, predicting_field=predicting_field)

    def evaluate_summary(self, evaluate_query: Dict) -> Dict:
        res = self._api.evaluate(client=self.client, query=evaluate_query)
        return {'train_samples': res.train_sample_count, 'test_samples': res.test_sample_count,
                'accuracy': res.accuracy}

    def send_query(self, api_method_name: str, query: Dict, use_job: bool) -> str:
        resp = getattr(self._api, api_method_name)(client=self.client, query=query, use_job=use_job)
        return resp.to_json_string(indent=2)


#: CLI query command -> the v2 endpoint its body is posted to, unchanged
V2_QUERY_ENDPOINTS = {
    'search': '/_search', 'predict': '/_predict', 'recommend': '/_recommend',
    'evaluate': '/_evaluate', 'match': '/_match', 'relate': '/_relate',
    'estimate': '/_estimate', 'aggregate': '/_aggregate', 'generic_query': '/_query',
}


class V2Backend:
    """the CLI commands on ``aito.v2.Client``: tables are v2 collections"""

    api_version = 'v2'

    def __init__(self, client):
        self.client = client

    def create_database(self, schema: Dict):
        # v2 has no whole-database create: one collection per table, in order
        tables = schema.get('schema', schema)
        for table_name, table_schema in tables.items():
            self.create_table(table_name, table_schema)

    def create_table(self, table_name: str, schema: Union[AitoTableSchema, Dict]):
        self.client.create_collection(table_name, _columns_of(schema))

    def table_schema_json(self, table_name: str) -> str:
        return json.dumps(self.client.get_schema(table_name), indent=2)

    def database_schema_json(self) -> str:
        return json.dumps(self.client.get_schema(), indent=2)

    def show_tables(self) -> List[str]:
        schema = self.client.get_schema()
        return sorted(schema.get('schema', schema))

    def delete_table(self, table_name: str):
        self.client.delete_collection(table_name)

    def delete_database(self):
        # no whole-database delete on v2: drop every collection and table in it
        for table_name in self.show_tables():
            self.client.delete_collection(table_name)

    def copy_table(self, table_name: str, copy_table_name: str, replace: bool):
        """the schema AND the rows, as v1's copy-table does

        On a v2 collection, ``/schema/_copy`` copies only the schema (measured on
        2.11.1: 0 of 10 rows arrive), while on a legacy table it copies the data
        too. So the rows are copied here, page by page, and the counts compared.
        """
        self.client.copy_schema({'from': table_name, 'copy': copy_table_name, 'replace': replace})
        if self.client.search(from_table=copy_table_name, limit=0).json.get('total'):
            return  # the engine copied the data (a legacy table)
        page, offset, copied = 1000, 0, 0
        while True:
            rows = self.client.search(from_table=table_name, limit=page, offset=offset).json.get('hits', [])
            if not rows:
                break
            copied += self.client.upload_entries(copy_table_name, rows)
            offset += len(rows)
        source_total = self.client.search(from_table=table_name, limit=0).json.get('total')
        if copied != source_total:
            raise RuntimeError(f"copy-table copied {copied} of {source_total} rows into `{copy_table_name}`")
        if copied:
            self.client.optimize(copy_table_name)

    def rename_table(self, old_name: str, new_name: str, replace: bool):
        raise NotSupportedOnV2(
            "rename-table has no v2 equivalent (the engine has no /api/v2/schema/_rename). "
            "Use copy-table, then delete-table, or pass --api-version v1")

    def upload_entries(self, table_name: str, entries: List[Dict]):
        self.client.upload_entries(table_name, entries)

    def optimize_table(self, table_name: str):
        self.client.optimize(table_name)

    def _table_schema_for_conversion(self, table_name: str) -> Optional[AitoTableSchema]:
        # the collection's columns drive type conversion exactly as a v1 table
        # schema does; v2-only column properties are not needed for that
        columns = _columns_of(self.client.get_schema(table_name))
        return AitoTableSchema.from_deserialized_object({'type': 'table', 'columns': columns})

    def upload_file(self, table_name: str, in_file_path: Path, in_format: str):
        import io
        df = DataFrameHandler().convert_file(
            read_input=in_file_path, write_output=io.StringIO(), in_format=in_format,
            out_format='json', use_table_schema=self._table_schema_for_conversion(table_name))
        self.client.upload_entries(table_name, _df_to_rows(df))

    def upload_data_frame(self, table_name: str, df, create_with_schema: Optional[AitoTableSchema] = None):
        if create_with_schema is not None:
            self.create_table(table_name, create_with_schema)
        self.client.upload_entries(table_name, _df_to_rows(df))

    def quick_add_table(self, input_file: Path, input_format: Optional[str], table_name: Optional[str]):
        import io
        in_f_path = Path(input_file)
        in_format = in_f_path.suffixes[0].replace('.', '') if input_format is None else input_format
        table_name = in_f_path.stem if table_name is None else table_name
        df = DataFrameHandler().convert_file(
            read_input=in_f_path, write_output=io.StringIO(), in_format=in_format, out_format='json')
        self.upload_data_frame(table_name, df, AitoTableSchema.infer_from_pandas_data_frame(df))
        self.client.optimize(table_name)

    def quick_predict_and_evaluate(self, from_table: str, predicting_field: str) -> Tuple[Dict, Dict]:
        """the v1 example queries, in v2 syntax: ``$value`` replaces ``feature``"""
        columns = _columns_of(self.client.get_schema(from_table))
        predicting_col = predicting_field.split('.')[0]
        if predicting_col not in columns:
            raise ValueError(f"table `{from_table}` does not have column `{predicting_col}`")
        hits = self.client.search(from_table=from_table, limit=1).json.get('hits', [])
        if not hits:
            raise ValueError(f"table `{from_table}` is empty, cannot generate example query")
        first = hits[0]
        where = {col: first.get(col) for col in columns if col != predicting_col}
        predict_query = {'from': from_table, 'where': where, 'predict': predicting_field,
                         'select': ['$p', '$value', '$why']}
        evaluate_query = {
            'test': {'$index': {'$mod': [10, 0]}},
            'evaluate': {'from': from_table, 'where': {col: {'$get': col} for col in where},
                         'predict': predicting_field},
        }
        return predict_query, evaluate_query

    def evaluate_summary(self, evaluate_query: Dict) -> Dict:
        res = self.client.evaluate(evaluate_query)
        return {'train_samples': res.train_sample_count, 'test_samples': res.test_sample_count,
                'accuracy': res.accuracy}

    def send_query(self, api_method_name: str, query: Dict, use_job: bool) -> str:
        if use_job:
            raise NotSupportedOnV2("--use-job is v1 only; v2 has no job endpoints")
        endpoint = V2_QUERY_ENDPOINTS.get(api_method_name)
        if endpoint is None:
            raise NotSupportedOnV2(
                f"{api_method_name.replace('_', '-')} has no v2 endpoint; pass --api-version v1")
        return json.dumps(self.client.request('POST', endpoint, query), indent=2)
