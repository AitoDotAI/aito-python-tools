"""Different APIs that takes an :class:`Aito Client object <aito.client.AitoClient>` as the first argument

"""

import logging
import tempfile
import time
from os import PathLike, unlink
from pathlib import Path
from typing import Dict, List, BinaryIO, Union, Tuple, Iterable

import ndjson
import requests as requestslib

from aito.client import AitoClient
from aito.schema import AitoDatabaseSchema, AitoTableSchema
from aito.utils._file_utils import gzip_file, check_file_is_gzipped
from aito.utils.data_frame_handler import DataFrameHandler

LOG = logging.getLogger('AitoAPI')


def get_version(client: AitoClient) -> str:
    """get the aito instance version

    :param client: the AitoClient instance
    :type client: AitoClient
    :return: version information in json format
    :rtype: Dict
    """
    resp = client.request(method='GET', endpoint='/version')
    return resp['version']


def create_database(client: AitoClient, database_schema: Union[AitoDatabaseSchema, Dict]):
    """`create a database <https://aito.ai/docs/api/#put-api-v1-schema>`__ using the specified database schema

    .. note::

        requires the client to be setup with the READ-WRITE API key

    :param client: the AitoClient instance
    :type client: AitoClient
    :param database_schema: Aito database schema
    :type database_schema: Dict
    :return: the database schema
    :rtype: Dict
    """
    if not isinstance(database_schema, AitoDatabaseSchema):
        database_schema = AitoDatabaseSchema.from_deserialized_object(database_schema)
    client.request(method='PUT', endpoint='/api/v1/schema', query=database_schema.to_json_serializable())
    LOG.info('database schema created')


def get_database_schema(client: AitoClient) -> AitoDatabaseSchema:
    """`get the schema of the database <https://aito.ai/docs/api/#get-api-v1-schema>`__

    :param client: the AitoClient instance
    :type client: AitoClient
    :return: Aito database schema
    :rtype: Dict
    """
    res = client.request(method='GET', endpoint='/api/v1/schema')
    return AitoDatabaseSchema.from_deserialized_object(res.json)


def delete_database(client: AitoClient):
    """`delete the whole database <https://aito.ai/docs/api/#delete-api-v1-schema>`__

    .. note::

        requires the client to be setup with the READ-WRITE API key

    :param client: the AitoClient instance
    :type client: AitoClient
    :return: deleted tables
    :rtype: Dict
    """
    client.request(method='DELETE', endpoint='/api/v1/schema')
    LOG.info('database deleted')


def create_table(client: AitoClient, table_name: str, table_schema: Union[AitoTableSchema, Dict]):
    """`create a table <https://aito.ai/docs/api/#put-api-v1-schema-table>`__
    with the specified table name and schema

    update the table if the table already exists and does not contain any data

    .. note::

        requires the client to be setup with the READ-WRITE API key

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table
    :type table_name: str
    :param table_schema: Aito table schema
    :type table_schema: an AitoTableSchema object or a Dict, optional
    :return: the table schema
    :rtype: Dict
    """
    if not isinstance(table_schema, AitoTableSchema):
        if not isinstance(table_schema, dict):
            raise ValueError("the input table schema must be either an AitoTableSchema object or a dict")
        table_schema = AitoTableSchema.from_deserialized_object(table_schema)
    client.request(method='PUT', endpoint=f'/api/v1/schema/{table_name}', query=table_schema.to_json_serializable())
    LOG.info(f'table `{table_name}` created')


def get_table_schema(client: AitoClient, table_name: str) -> AitoTableSchema:
    """`get the schema of the specified table <https://aito.ai/docs/api/#get-api-v1-schema-table>`__

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table
    :type table_name: str
    :return: the table schema
    :rtype: AitoTableSchema
    """
    resp = client.request(method='GET', endpoint=f'/api/v1/schema/{table_name}')
    return AitoTableSchema.from_deserialized_object(resp.json)


def delete_table(client: AitoClient, table_name: str):
    """`delete the specified table <https://aito.ai/docs/api/#delete-api-v1-schema>`__

    .. note::

        requires the client to be setup with the READ-WRITE API key

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table
    :type table_name: str
    :return: deleted table
    :rtype: Dict
    """
    client.request(method='DELETE', endpoint=f'/api/v1/schema/{table_name}')
    LOG.info(f'table `{table_name}` deleted')


def get_existing_tables(client: AitoClient) -> List[str]:
    """get a list of existing tables in the instance

    :param client: the AitoClient instance
    :type client: AitoClient
    :return: list of the names of existing tables
    :rtype: List[str]
    """
    db_schema = get_database_schema(client)
    return db_schema.tables


def check_table_exists(client: AitoClient, table_name: str) -> bool:
    """check if a table exists in the instance

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table
    :type table_name: str
    :return: True if the table exists
    :rtype: bool
    """
    existing_tables = get_existing_tables(client)
    return table_name in existing_tables


def rename_table(client: AitoClient, old_name: str, new_name: str, replace: bool = False):
    """`rename a table <https://aito.ai/docs/api/#post-api-v1-schema-rename>`__

    .. note::

        requires the client to be setup with the READ-WRITE API key

    :param client: the AitoClient instance
    :type client: AitoClient
    :param old_name: the name of the table to be renamed
    :type old_name: str
    :param new_name: the new name of the table
    :type new_name: str
    :param replace: replace an existing table of which name is the new name, defaults to False
    :type replace: bool, optional
    """
    client.request(
        method='POST',
        endpoint='/api/v1/schema/_rename',
        query={'from': old_name, 'rename': new_name, 'replace': replace}
    )


def copy_table(client: AitoClient, table_name: str, copy_table_name: str, replace: bool = False):
    """`copy a table <https://aito.ai/docs/api/#post-api-v1-schema-copy>`__

    .. note::

        requires the client to be setup with the READ-WRITE API key

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table to be copied
    :type table_name: str
    :param copy_table_name: the name of the new copy table
    :type copy_table_name: str
    :param replace: replace an existing table of which name is the name of the copy table, defaults to False
    :type replace: bool, optional
    """
    client.request(
        method='POST',
        endpoint='/api/v1/schema/_copy',
        query={'from': table_name, 'copy': copy_table_name, 'replace': replace}
    )


def optimize_table(client: AitoClient, table_name):
    """`optimize <https://aito.ai/docs/api/#post-api-v1-data-table-optimize>`__
    the specified table after uploading the data

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table
    :type table_name: str
    :return:
    :rtype:
    """
    try:
        client.request(method='POST', endpoint=f'/api/v1/data/{table_name}/optimize', query={})
    except Exception as e:
        LOG.error(f'failed to optimize: {e}')
    LOG.info(f'table {table_name} optimized')


def upload_entries(
        client: AitoClient,
        table_name: str,
        entries: Iterable[Dict],
        batch_size: int = 1000,
        optimize_on_finished: bool = True
):
    """populate table entries by batches of batch_size

    .. note::

        requires the client to be setup with the READ-WRITE API key

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table
    :type table_name: str
    :param entries: iterable of the table entries
    :type entries: Iterable[Dict]
    :param batch_size: the batch size, defaults to 1000
    :type batch_size: int, optional
    :param optimize_on_finished: `optimize <https://aito.ai/docs/api/#post-api-v1-data-table-optimize>`__
        the table on finished, defaults to True
    :type optimize_on_finished: bool

    Upload a Pandas DataFrame

    >>> import pandas as pd
    >>> df = pd.DataFrame({'height': [94, 170], 'weight': [31, 115], 'depth':  [ 3,  29]})
    >>> entries = df.to_dict(orient='records')
    >>> client.upload_entries(table_name='specifications', entries=entries) # doctest: +SKIP

    Upload a generator of entries

    >>> def entries_generator(start, end):
    ...     for idx in range(start,clea end):
    ...         entry = {'id': idx}
    ...         yield entry
    >>> client.upload_entries(
    ...     table_name="table_name",
    ...     entries=entries_generator(start=0, end=10000),
    ...     batch_size=500,
    ...     optimize_on_finished=False
    ... ) # doctest: +SKIP
    """

    LOG.info(f'uploading entries to table `{table_name}` with batch size of {batch_size}...')
    begin_index, last_index, populated_count = 0, 0, 0
    entries_batch = []

    def _upload_a_batch(begin_idx, last_idx, populated_c, batch_content):
        try:
            LOG.debug(f'uploading batch {begin_idx}:{last_idx}...')
            client.request(method='POST', endpoint=f"/api/v1/data/{table_name}/batch", query=batch_content)
            populated_c += len(batch_content)
            LOG.info(f'uploaded batch {begin_idx}:{last_idx}')
        except Exception as e:
            LOG.error(f'batch {begin_idx}:{last_idx} failed: {e}')
        return populated_c

    for entry in entries:
        entries_batch.append(entry)
        last_index += 1

        if last_index % batch_size == 0:
            populated_count = _upload_a_batch(begin_index, last_index, populated_count, entries_batch)

            begin_index = last_index
            entries_batch = []

    # upload the remaining entries
    if last_index % batch_size != 0:
        populated_count = _upload_a_batch(begin_index, last_index, populated_count, entries_batch)

    if populated_count == 0:
        raise Exception("failed to upload any data into Aito")

    LOG.info(f'uploaded {populated_count}/{last_index} entries to table `{table_name}`')

    if optimize_on_finished:
        optimize_table(client, table_name)


def initiate_upload_file(client: AitoClient, table_name: str) -> Dict:
    """`Initial uploading a file to a table <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table to be uploaded
    :type table_name: str
    :return: The details to execute the S3 upload and the upload session's id
    :rtype: Dict
    """
    LOG.debug('initiating file upload...')
    r = client.request(method='POST', endpoint=f"/api/v1/data/{table_name}/file")
    return r.json


def upload_binary_file_to_s3(initiate_upload_file_response: Dict, binary_file: BinaryIO):
    """Upload a binary file to AWS S3 with using the information from :func:`.initiate_upload_file`

    :param initiate_upload_file_response: the result from :func:`.initiate_upload_file`
    :type initiate_upload_file_response: Dict
    :param binary_file: binary file object
    :type binary_file: BinaryIO
    """
    LOG.debug('uploading binary file to S3...')
    LOG.debug('getting session id and upload url...')
    s3_url = initiate_upload_file_response['url']
    upload_req_method = initiate_upload_file_response['method']
    LOG.debug('uploading file file to S3...')
    try:
        r = requestslib.request(upload_req_method, s3_url, data=binary_file)
        r.raise_for_status()
    except Exception as e:
        raise Exception(f'failed to upload file to S3: {e}')
    LOG.debug('uploaded file to S3')


def trigger_file_processing(client: AitoClient, table_name: str, upload_session_id: str):
    """`Trigger file processing of uploading a file to a table
    <https://aito.ai/docs/api/#post-api-v1-data-table-file-uuid>`__

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table to be uploaded
    :type table_name: str
    :param upload_session_id: The upload session id from :func:`.initiate_upload_file`
    :type upload_session_id: str
    """
    LOG.debug('triggering file processing...')
    session_end_point = f'/api/v1/data/{table_name}/file/{upload_session_id}'
    client.request(method='POST', endpoint=session_end_point)
    LOG.info('triggered file processing')


def poll_file_processing_status(client: AitoClient, table_name: str, upload_session_id: str, polling_time: int = 10):
    """Polling the `file processing status <https://aito.ai/docs/api/#get-api-v1-data-table-file-uuid>`__ until
    the processing finished

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table to be uploaded
    :type table_name: str
    :param upload_session_id: The upload session id from :func:`.initiate_upload_file`
    :type upload_session_id: str
    :param polling_time: polling wait time
    :type polling_time: int
    """
    LOG.debug('polling processing status...')
    session_end_point = f'/api/v1/data/{table_name}/file/{upload_session_id}'
    while True:
        try:
            processing_progress_resp = client.request(method='GET', endpoint=session_end_point)
            status = processing_progress_resp['status']
            LOG.debug(f"completed count: {status['completedCount']}, throughput: {status['throughput']}")
            if processing_progress_resp['errors']['message'] != 'Last 0 failing rows':
                LOG.error(processing_progress_resp['errors'])
            if status['finished']:
                break
        except Exception as e:
            LOG.debug(f'failed to get file upload status: {e}')
        time.sleep(polling_time)


def upload_binary_file(
        client: AitoClient,
        table_name: str,
        binary_file: BinaryIO,
        polling_time: int = 10,
        optimize_on_finished: bool = True
):
    """`upload a binary file object to a table <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__

    .. note::

        requires the client to be setup with the READ-WRITE API key

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table to be uploaded
    :type table_name: str
    :param binary_file: binary file object
    :type binary_file: BinaryIO
    :param polling_time: polling wait time
    :type polling_time: int
    :param optimize_on_finished: :func:`optimize_table` when finished uploading, defaults to True
    :type optimize_on_finished: bool
    """
    LOG.debug(f'uploading file object to table `{table_name}`...')
    init_upload_resp = initiate_upload_file(client=client, table_name=table_name)
    upload_binary_file_to_s3(initiate_upload_file_response=init_upload_resp, binary_file=binary_file)
    upload_session_id = init_upload_resp['id']
    trigger_file_processing(client=client, table_name=table_name, upload_session_id=upload_session_id)
    poll_file_processing_status(
        client=client, table_name=table_name, upload_session_id=upload_session_id, polling_time=polling_time
    )

    LOG.info(f'uploaded file object to table `{table_name}`')
    if optimize_on_finished:
        optimize_table(client, table_name)


def upload_file(
        client: AitoClient,
        table_name: str,
        file_path: PathLike,
        polling_time: int = 10,
        optimize_on_finished: bool = True
):
    """`upload a file <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__ to the specfied table

    .. note::

        requires the client to be setup with the READ-WRITE API key

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table
    :type table_name: str
    :param file_path: path to the file to be uploaded
    :type file_path: PathLike
    :param polling_time: polling wait time
    :type polling_time: int
    :param optimize_on_finished: :func:`optimize_table` when finished uploading, defaults to True
    :type optimize_on_finished: bool
    :raises ValueError: incorrect file extension, should be .ndjson.gz
    """
    if not check_file_is_gzipped(file_path):
        raise ValueError(f'file {file_path} is not a gzip-compressed ndjson file')
    with open(file_path, 'rb') as f:
        upload_binary_file(
            client=client,
            table_name=table_name,
            binary_file=f,
            polling_time=polling_time,
            optimize_on_finished=optimize_on_finished
        )


def quick_add_table(
        client: AitoClient, input_file: Union[Path, PathLike], table_name: str = None, input_format: str = None
):
    """Create a table and upload a file to the table, using the default inferred schema

    :param client: the AitoClient instance
    :type client: AitoClient
    :param input_file: path to the input file to be uploaded
    :type input_file: Union[Path, PathLike]
    :param table_name: the name of the table, defaults to the name of the input file
    :type table_name: Optional[str]
    :param input_format: specify the format of the input file, defaults to the input file extension
    :type input_format: Optional[str]
    """
    df_handler = DataFrameHandler()

    try:
        in_f_path = Path(input_file)
    except Exception:
        raise ValueError(f'invalid path: {input_file}')
    in_format = in_f_path.suffixes[0].replace('.', '') if input_format is None else input_format
    if in_format not in df_handler.allowed_format:
        raise ValueError(f'invalid file format {in_format}. Must be one of {"|".join(df_handler.allowed_format)}')

    table_name = in_f_path.stem if table_name is None else table_name

    converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=False)

    converted_df = df_handler.convert_file(
        read_input=in_f_path,
        write_output=converted_tmp_file.name,
        in_format=in_format,
        out_format='ndjson',
        convert_options={'compression': 'gzip'}
    )
    converted_tmp_file.close()

    inferred_schema = AitoTableSchema.infer_from_pandas_data_frame(converted_df)
    create_table(client, table_name, inferred_schema)

    with open(converted_tmp_file.name, 'rb') as in_f:
        upload_binary_file(client=client, table_name=table_name, binary_file=in_f)
    converted_tmp_file.close()
    unlink(converted_tmp_file.name)


def create_job(client: AitoClient, job_endpoint: str, query: Union[List, Dict]) -> Dict:
    """Create a `job <https://aito.ai/docs/api/#post-api-v1-jobs-query>`__
    for a query that takes longer than 30 seconds to run

    :param client: the AitoClient instance
    :type client: AitoClient
    :param job_endpoint: job endpoint
    :type job_endpoint: str
    :param query: the query for the endpoint
    :type query: Union[List, Dict]
    :return: job information
    :rtype: Dict
    """
    resp = client.request(method='POST', endpoint=job_endpoint, query=query)
    return resp.json


def get_job_status(client: AitoClient, job_id: str) -> Dict:
    """`Get the status of a job <https://aito.ai/docs/api/#get-api-v1-jobs-uuid>`__ with the specified job id

    :param client: the AitoClient instance
    :type client: AitoClient
    :param job_id: the id of the job
    :type job_id: str
    :return: job status
    :rtype: Dict
    """
    resp = client.request(method='GET', endpoint=f'/api/v1/jobs/{job_id}')
    return resp.json


def get_job_result(client: AitoClient, job_id: str) -> Dict:
    """`Get the result of a job <https://aito.ai/docs/api/#get-api-v1-jobs-uuid-result>`__ with the specified job id

    :param client: the AitoClient instance
    :type client: AitoClient
    :param job_id: the id of the job
    :type job_id: str
    :return: the job result
    :rtype: Dict
    """
    resp = client.request(method='GET', endpoint=f'/api/v1/jobs/{job_id}/result')
    return resp.json


def job_request(
        client: AitoClient, job_endpoint: str, query: Union[Dict, List] = None, polling_time: int = 10
) -> Dict:
    """make a request to an Aito API endpoint using job

    This method should be used for requests that take longer than 30 seconds, e.g: evaluate

    The following query evaluate the performance of a predict query that uses the name of
    a product to predict its category

    >>> response = client.job_request(
    ...     job_endpoint='/api/v1/jobs/_evaluate',
    ...     query={
    ...         "test": { "$index": { "$mod": [4, 0] } },
    ...         "evaluate": {
    ...             "from": "products",
    ...             "where": { "name": { "$get": "name" } },
    ...             "predict": "category"
    ...         }
    ...     }
    ... )
    >>> print(response["accuracy"]) # doctest: +ELLIPSIS
    0.72...

    :param client: the AitoClient instance
    :type client: AitoClient
    :param job_endpoint: job end point
    :type job_endpoint: str
    :param query: an Aito query, defaults to None
    :type query: Union[Dict, List], optional
    :param polling_time: polling wait time, defaults to 10
    :type polling_time: int
    :raises RequestError: an error occurred during the execution of the job
    :return: request JSON content
    :rtype: Dict
    """
    resp = create_job(client, job_endpoint, query)
    job_id = resp['id']
    LOG.debug('polling job status...')
    while True:
        job_status_resp = get_job_status(client, job_id)
        if 'finishedAt' in job_status_resp:
            break
        time.sleep(polling_time)
    return get_job_result(client, job_id)


def get_table_size(client: AitoClient, table_name: str) -> int:
    """return the number of entries of the specified table

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table
    :type table_name: str
    :return: the number of entries in the table
    :rtype: int
    """
    resp = client.query({'from': table_name})
    return resp['total']


def query_entries(
        client: AitoClient, table_name: str, offset: int = 0, limit: int = 10, select: List[str] = None
) -> List[Dict]:
    """`query <https://aito.ai/docs/api/#post-api-v1-query>`__ entries of the specified table

    use offset and limit for `pagination <https://aito.ai/docs/api/#pagination>`__

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table
    :type table_name: str
    :param offset: the offset of the first entry, defaults to 0
    :type offset: int, optional
    :param limit: the number of entries to be returned, defaults to 10
    :type limit: int, optional
    :param select: specify the fields of the entry, including link fields, to be returned
    :type select: List[str], optional
    :return: the table entries
    :rtype: List[Dict]
    """
    resp = client.query({'from': table_name, 'offset': offset, 'limit': limit, 'select': select})
    return resp['hits']


def query_all_entries(
        client: AitoClient,
        table_name: str,
        select: List[str] = None,
        batch_size: int = 5000
) -> List[Dict]:
    """`query <https://aito.ai/docs/api/#post-api-v1-query>`__  all entries of the specified table

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table
    :type table_name: str
    :param select: specify the fields of the entry, including link fields, to be returned
    :type select: List[str], optional
    :param batch_size: the number of entries to be queried at once, defaults to 5000
    :type batch_size: int, optional
    :return: list of all entries in the table
    :rtype: List[Dict]
    """
    LOG.debug(f'getting all entries of table {table_name} in batch of {batch_size}...')
    table_size = get_table_size(client, table_name)
    all_entries = []
    begin_idx = 0
    while begin_idx < table_size:
        last_idx = begin_idx + batch_size if begin_idx + batch_size <= table_size else table_size
        LOG.debug(f'getting table chunk {begin_idx}:{last_idx}...')
        all_entries += query_entries(client, table_name, offset=begin_idx, limit=batch_size, select=select)
        LOG.debug(f'queried chunk {begin_idx}:{last_idx}')
        begin_idx += batch_size
    LOG.info(f'queried all entries of table `{table_name}`')
    return all_entries


def download_table(
        client: AitoClient,
        table_name: str,
        output_folder: PathLike,
        file_name: str = None,
        batch_size: int = 5000,
        gzip_output: bool = False
):
    """download a table to a NDJSON file or a gzipped NDJSON file

    :param client: the AitoClient instance
    :type client: AitoClient
    :param table_name: the name of the table
    :type table_name: str
    :param output_folder: the folder where the output file is written to
    :type output_folder: PathLike
    :parm file_name: the name of the output file, defaults to None in which the table name is used the file name
    :type file_name: str
    :param batch_size: the number of entries to be downloaded at once, defaults to 5000
    :type batch_size: int
    :param gzip_output: gzip the output file, defaults to False
    :type gzip_output: bool
    """
    if not file_name:
        file_name = table_name
    out_file_path = Path(output_folder) / f'{file_name}.ndjson'
    if out_file_path.exists():
        LOG.warning(f'output file {out_file_path} already exists')
    LOG.debug(f'downloading table `{table_name}` to {out_file_path}')
    table_size = get_table_size(client, table_name)
    begin_idx = 0
    while begin_idx < table_size:
        last_idx = begin_idx + batch_size if begin_idx + batch_size <= table_size else table_size
        LOG.debug(f'downloading table chunk {begin_idx}:{last_idx}...')
        entries_batch = query_entries(client=client, table_name=table_name, offset=begin_idx, limit=batch_size)
        with out_file_path.open('a+') as f:
            ndjson.dump(entries_batch, f)
            if last_idx != table_size:
                f.write('\n')
        LOG.debug(f'downloaded table chunk {begin_idx}:{last_idx}')
        begin_idx += batch_size
    if gzip_output:
        gzip_file(out_file_path, keep=False)
    LOG.info(f'downloaded table `{table_name}` to {out_file_path}')


def quick_predict_and_evaluate(
        client: AitoClient,
        from_table: str,
        predicting_field: str
) -> Tuple[Dict, Dict]:
    """generate an example predict query to predict a field and the corresponding evaluate query

    The example query will use all fields of the table as the hypothesis and the first entry of the table as the
    input data

    :param client: the AitoClient instance
    :type client: AitoClient
    :param from_table: the name of the table the will be use as context for prediction.
    :type from_table: str
    :param predicting_field: the name of the predicting field. If the field belongs to a linked table,
        it should be in the format of <column_with_link>.<field_name>
    :type predicting_field: str
    :return: a tuple contains the predict query and the prediction result
    :rtype: Tuple[Dict, Dict]
    """
    database_schema = get_database_schema(client)
    table_schema = database_schema[from_table]

    predicting_field_splitted = predicting_field.split('.')
    is_predicting_a_linked_field = len(predicting_field_splitted) == 2

    predicting_col = predicting_field if not is_predicting_a_linked_field else predicting_field_splitted[0]
    if predicting_col not in table_schema.columns:
        raise ValueError(f"table `{from_table}` does not have column `{predicting_col}`")
    if is_predicting_a_linked_field:
        linked_col = predicting_field_splitted[1]
        linked_table = table_schema.links[predicting_col].table_name
        if linked_col not in database_schema[linked_table].columns:
            raise ValueError(f"linked table `{linked_table}` does not have column `{linked_col}`")

    table_first_entry_res = query_entries(client, from_table, limit=1)
    if not table_first_entry_res:
        raise ValueError(f"table `{from_table}` is empty, cannot generate example query")
    table_first_entry = table_first_entry_res[0]

    predict_query = {
        'from': from_table,
        'where': {
            col: table_first_entry.get(col) for col in table_schema.columns if col != predicting_col
        },
        'predict': predicting_field,
        'select': ['$p', 'feature', '$why']
    }

    pred_query_with_get_op = dict(predict_query)
    for col in pred_query_with_get_op['where']:
        pred_query_with_get_op['where'][col] = {'$get': col}

    evaluate_query = {
        'test': {'$index': {"$mod": [10, 0]}},
        'evaluate': pred_query_with_get_op
    }

    return predict_query, evaluate_query


def quick_predict(
        client: AitoClient,
        from_table: str,
        predicting_field: str
):
    """generate an example predict query to predict a field

    The example query will use all fields of the table as the hypothesis and the first entry of the table as the
    input data

    :param client: the AitoClient instance
    :type client: AitoClient
    :param from_table: the name of the table the will be use as context for prediction.
    :type from_table: str
    :param predicting_field: the name of the predicting field. If the field belongs to a linked table,
        it should be in the format of <column_with_link>.<field_name>
    :type predicting_field: str
    :return: The example predict query
    :rtype: Dict
    """
    predict_query, evaluate_query = quick_predict_and_evaluate(
        client=client, from_table=from_table, predicting_field=predicting_field
    )
    return predict_query
