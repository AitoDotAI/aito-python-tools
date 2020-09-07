"""A versatile client that connects to the Aito Database Instance

"""

import asyncio
import logging
import time
import warnings
from os import PathLike
from pathlib import Path
from typing import Dict, List, BinaryIO, Union, Tuple, Iterable, Optional

import ndjson
import requests as requestslib
from aiohttp import ClientSession, ClientResponseError

from aito.schema import AitoTableSchema, AitoDatabaseSchema
from aito.utils._file_utils import gzip_file, check_file_is_gzipped
from .client_request import BaseRequest

LOG = logging.getLogger('AitoClient')


class BaseError(Exception):
    """An error occurred when using the client

    """
    def __init__(self, message: str):
        LOG.error(message)
        LOG.debug(message, stack_info=True)
        self.message = message

    def __str__(self):
        return self.message


class RequestError(BaseError):
    """An error occurred when sending a request to the Aito instance

    """
    def __init__(self, request: BaseRequest, error: Exception):
        if isinstance(error, requestslib.HTTPError):
            resp = error.response.json()
            error_msg = resp['message'] if 'message' in resp else resp
        elif isinstance(error, ClientResponseError):
            error_msg = error.message
        else:
            error_msg = str(error)
        super().__init__(f'failed to {request}: {error_msg}')


class AitoClient:
    """A versatile client that connects to the Aito Database Instance

    """
    def __init__(self, instance_url: str, api_key: str, check_credentials: Optional[bool] = True):
        """

        :param instance_url: Aito instance url
        :type instance_url: str
        :param api_key: Aito instance API key
        :type api_key: str
        :param check_credentials: check the given credentials by requesting the Aito instance version, defaults to True
        :type check_credentials: Optional[bool]
        :raises BaseError: an error occurred during the creation of AitoClient
        """
        self.url = instance_url.strip("/")
        self.headers = {'Content-Type': 'application/json', 'x-api-key': api_key}
        if check_credentials:
            try:
                self.get_version()
            except Exception:
                raise BaseError('failed to instantiate Aito Client, please check your credentials')

    def request(self, request: BaseRequest) -> Dict:
        """make a request to an Aito API endpoint
        The client returns a JSON response if the request succeed and a :class:`.RequestError` if the request failed

        :param request: request object
        :type request: BaseRequest
        :raises RequestError: an error occurred during the execution of the request
        :return: request JSON content
        :rtype: Dict

        Simple request to get the schema of a table:

        >>> res = client.request(BaseRequest(method="GET", endpoint="/api/v1/schema/impressions"))
        >>> pprint(res) # doctest: +NORMALIZE_WHITESPACE
         {'columns': {'product': {'link': 'products.id',
                                 'nullable': False,
                                 'type': 'String'},
                     'purchase': {'nullable': False, 'type': 'Boolean'},
                     'session': {'link': 'sessions.id',
                                 'nullable': False,
                                 'type': 'String'}},
         'type': 'table'}

         Sends a `PREDICT <https://aito.ai/docs/api/#post-api-v1-predict>`__ query:

         >>> client.request(
         ...    BaseRequest(
         ...        method="POST",
         ...        endpoint="/api/v1/_predict",
         ...        query={
         ...            "from": "impressions",
         ...            "where": { "session": "veronica" },
         ...            "predict": "product.name",
         ...            "limit": 1
         ...        }
         ...    )
         ... ) # doctest: +NORMALIZE_WHITESPACE
         {'offset': 0, 'total': 142, 'hits': [{'$p': 0.07285448674038553, 'field': 'product.name', 'feature': 'pirkka'}]}

         Returns an error when make a request to an incorrect path:

         >>> client.request(BaseRequest(method="GET", endpoint="/api/v1/incorrect-path")) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
         Traceback (most recent call last):
            ...
         aito.client.RequestError: failed to GET(/api/v1/incorrect-path): None: The path you requested [/incorrect-path] does not exist
         """
        try:
            resp = requestslib.request(
                method=request.method,
                url=self.url + request.endpoint,
                headers=self.headers,
                json=request.query
            )
            resp.raise_for_status()
            json_resp = resp.json()
        except Exception as e:
            raise RequestError(request, e)
        return json_resp

    async def async_request(
            self, session: ClientSession, request: BaseRequest
    ) -> Tuple[int, Union[Dict, List]]:
        """execute a request asynchronously using aiohttp ClientSession

        :param session: aiohttp ClientSession for making request
        :type session: ClientSession
        :param request: the request object
        :type request: BaseRequest
        """
        LOG.debug(f'async {request}')
        async with session.request(
                method=request.method,
                url=self.url + request.endpoint,
                json=request.query,
                headers=self.headers) as resp:
            return resp.status, await resp.json()

    async def bounded_async_request(self, semaphore: asyncio.Semaphore, *args) -> Tuple[int, Union[Dict, List]]:
        """bounded concurrent requests with asyncio semaphore

        :param semaphore: asyncio Semaphore
        :type semaphore: asyncio.Semaphore
        :param args: :func:`.async_request` arguments
        :return: tuple of request status code and request json content
        :rtype: Tuple[int, Union[Dict, List]]
        """
        async with semaphore:
            return await self.async_request(*args)

    def async_requests(
            self,
            methods: List[str],
            endpoints: List[str],
            queries: List[Union[List, Dict]],
            batch_size: int = 10
    ) -> List[Dict]:
        """
        .. deprecated:: 0.4.0

        Use :func:`batch_requests` instead
        """
        warnings.warn(
            'The AitoClient.async_requests function is deprecated and will be removed '
            'in a future version. Use AitoClient.batch_requests instead',
            category=FutureWarning
        )
        requests = [BaseRequest(method, endpoints[idx], queries[idx]) for idx, method in enumerate(methods)]
        return self.batch_requests(requests=requests, max_concurrent_requests=batch_size)

    def batch_requests(
            self,
            requests: List[BaseRequest],
            max_concurrent_requests: int = 10
    ) -> List[Dict]:
        """execute a batch of requests asynchronously

        This method is useful when sending a batch of requests, for example, when sending a batch of predict requests.

        :param requests: list of request object
        :type requests: List[BaseRequest]
        :param max_concurrent_requests: the number of queries to be sent per batch
        :type max_concurrent_requests: int
        :return: list of request response or exception if a request did not succeed
        :rtype: List[Dict]

        Find products that multiple users would most likely buy

        >>> users = ['veronica', 'larry', 'alice']
        >>> responses = client.batch_requests([
        ...     BaseRequest(
        ...         method='POST',
        ...         endpoint='/api/v1/_match',
        ...         query = {
        ...             'from': 'impressions',
        ...             'where': { 'session.user': usr },
        ...             'match': 'product'
        ...         }
        ...     )
        ...     for usr in users
        ... ])
        >>> # Print top product for each customer
        >>> for idx, usr in enumerate(users):
        ...     print(f"{usr}: {responses[idx]['hits'][0]}") # doctest: +NORMALIZE_WHITESPACE +REPORT_UDIFF
        veronica: {'$p': 0.14496525949529243, 'category': '100', 'id': '6410405060457', 'name': 'Pirkka bio cherry tomatoes 250g international 1st class', 'price': 1.29, 'tags': 'fresh vegetable pirkka tomato'}
        larry: {'$p': 0.2348757987154449, 'category': '104', 'id': '6410405216120', 'name': 'Pirkka lactose-free semi-skimmed milk drink 1l', 'price': 1.25, 'tags': 'lactose-free drink pirkka'}
        alice: {'$p': 0.11144746333281082, 'category': '104', 'id': '6408430000258', 'name': 'Valio eila™ Lactose-free semi-skimmed milk drink 1l', 'price': 1.95, 'tags': 'lactose-free drink'}

        """
        async def run():
            async with ClientSession() as session:
                tasks = [self.bounded_async_request(semaphore, session, req) for req in requests]
                return await asyncio.gather(*tasks, return_exceptions=True)

        semaphore = asyncio.Semaphore(max_concurrent_requests)
        loop = asyncio.get_event_loop()
        responses = loop.run_until_complete(run())
        for idx, resp in enumerate(responses):
            req = requests[idx]
            if isinstance(resp, Exception):
                responses[idx] = RequestError(req, resp)
            # manually handling raise for status since aiohttp doesnt return response content with raise for status
            elif resp[0] >= 400:
                responses[idx] = RequestError(req, resp[1])
            else:
                responses[idx] = resp[1]
        return responses

    def get_version(self) -> Dict:
        """get the aito instance version

        :return: version information in json format
        :rtype: Dict
        """
        return self.request(BaseRequest('GET', '/version'))

    def create_database(self, database_schema: Dict) -> Dict:
        """`create a database <https://aito.ai/docs/api/#put-api-v1-schema>`__ using the specified database schema

        .. note::

            requires the client to be setup with the READ-WRITE API key

        :param database_schema: Aito database schema
        :type database_schema: Dict
        :return: the database schema
        :rtype: Dict
        """
        r = self.request(BaseRequest('PUT', '/api/v1/schema', database_schema))
        LOG.info('database schema created')
        return r

    def get_database_schema(self) -> AitoDatabaseSchema:
        """`get the schema of the database <https://aito.ai/docs/api/#get-api-v1-schema>`__

        :return: Aito database schema
        :rtype: Dict
        """
        json_response = self.request(BaseRequest('GET', '/api/v1/schema'))
        return AitoDatabaseSchema.from_deserialized_object(json_response)

    def delete_database(self) -> Dict:
        """`delete the whole database <https://aito.ai/docs/api/#delete-api-v1-schema>`__

        .. note::

            requires the client to be setup with the READ-WRITE API key


        :return: deleted tables
        :rtype: Dict
        """
        r = self.request(BaseRequest('DELETE', '/api/v1/schema'))
        LOG.info('database deleted')
        return r

    def create_table(self, table_name: str, table_schema: Union[AitoTableSchema, Dict]) -> Dict:
        """`create a table <https://aito.ai/docs/api/#put-api-v1-schema-table>`__ with the specified table name and schema

        update the table if the table already exists and does not contain any data

        .. note::

            requires the client to be setup with the READ-WRITE API key

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
        r = self.request(BaseRequest('PUT', f'/api/v1/schema/{table_name}', table_schema.to_json_serializable()))
        LOG.info(f'table `{table_name}` created')
        return r

    def get_table_schema(self, table_name: str) -> AitoTableSchema:
        """`get the schema of the specified table <https://aito.ai/docs/api/#get-api-v1-schema-table>`__

        :param table_name: the name of the table
        :type table_name: str
        :return: the table schema
        :rtype: AitoTableSchema
        """
        json_response = self.request(BaseRequest('GET', f'/api/v1/schema/{table_name}'))
        return AitoTableSchema.from_deserialized_object(json_response)

    def delete_table(self, table_name: str) -> Dict:
        """`delete the specified table <https://aito.ai/docs/api/#delete-api-v1-schema>`__

        .. note::

            requires the client to be setup with the READ-WRITE API key

        :param table_name: the name of the table
        :type table_name: str
        :return: deleted table
        :rtype: Dict
        """
        r = self.request(BaseRequest('DELETE', f'/api/v1/schema/{table_name}'))
        LOG.info(f'table `{table_name}` deleted')
        return r

    def get_existing_tables(self) -> List[str]:
        """get a list of existing tables in the instance

        :return: list of the names of existing tables
        :rtype: List[str]
        """
        return self.get_database_schema().tables

    def check_table_exists(self, table_name: str) -> bool:
        """check if a table exists in the instance

        :param table_name: the name of the table
        :type table_name: str
        :return: True if the table exists
        :rtype: bool
        """
        return table_name in self.get_existing_tables()

    def rename_table(self, old_name: str, new_name: str, replace: bool = False):
        """`rename a table <https://aito.ai/docs/api/#post-api-v1-schema-rename>`__

        .. note::

            requires the client to be setup with the READ-WRITE API key

        :param old_name: the name of the table to be renamed
        :type old_name: str
        :param new_name: the new name of the table
        :type new_name: str
        :param replace: replace an existing table of which name is the new name, defaults to False
        :type replace: bool, optional
        """
        self.request(BaseRequest(
            'POST', '/api/v1/schema/_rename', {'from': old_name, 'rename': new_name, 'replace': replace}
        ))

    def copy_table(self, table_name: str, copy_table_name: str, replace: bool = False):
        """`copy a table <https://aito.ai/docs/api/#post-api-v1-schema-copy>`__

        .. note::

            requires the client to be setup with the READ-WRITE API key

        :param table_name: the name of the table to be copied
        :type table_name: str
        :param copy_table_name: the name of the new copy table
        :type copy_table_name: str
        :param replace: replace an existing table of which name is the name of the copy table, defaults to False
        :type replace: bool, optional
        """
        self.request(BaseRequest(
            'POST',
            '/api/v1/schema/_copy',
            {'from': table_name, 'copy': copy_table_name, 'replace': replace}
        ))

    def optimize_table(self, table_name):
        """`optimize the specified table after uploading the data <https://aito.ai/docs/api/#post-api-v1-data-table-optimize>`__

        :param table_name: the name of the table
        :type table_name: str
        :return:
        :rtype:
        """
        try:
            self.request(BaseRequest('POST', f'/api/v1/data/{table_name}/optimize', {}))
        except Exception as e:
            LOG.error(f'failed to optimize: {e}')
        LOG.info(f'table {table_name} optimized')

    def upload_entries_by_batches(
            self,
            table_name: str,
            entries: Iterable[Dict],
            batch_size: int = 1000,
            optimize_on_finished: bool = True
    ):
        """
        .. deprecated:: 0.2.1

        Use :func:`upload_entries` instead
        """
        warnings.warn(
            'The AitoClient.upload_entries_by_batches is deprecated and will be removed '
            'in a future version. Use AitoClient.upload_entries instead',
            category=FutureWarning
        )
        self.upload_entries(table_name, entries, batch_size, optimize_on_finished)

    def upload_entries(
            self,
            table_name: str,
            entries: Iterable[Dict],
            batch_size: int = 1000,
            optimize_on_finished: bool = True
    ):
        """populate table entries by batches of batch_size

        .. note::

            requires the client to be setup with the READ-WRITE API key

        :param table_name: the name of the table
        :type table_name: str
        :param entries: iterable of the table entries
        :type entries: Iterable[Dict]
        :param batch_size: the batch size, defaults to 1000
        :type batch_size: int, optional
        :param optimize_on_finished: `optimize <https://aito.ai/docs/api/#post-api-v1-data-table-optimize>`__ the table on finished, defaults to True
        :type optimize_on_finished: bool

        Upload a Pandas DataFrame

        >>> import pandas as pd
        >>> df = pd.DataFrame({'height': [94, 170], 'weight': [31, 115], 'depth':  [ 3,  29]})
        >>> entries = df.to_dict(orient='records')
        >>> client.upload_entries(table_name='specifications', entries=entries) # doctest: +SKIP

        Upload a genator of entries
        >>> def entries_generator(start, end):
        ...     for idx in range(start, end):
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

        begin_idx = 0
        populated = 0
        last_idx = 0
        entries_batch = []

        for entry in entries:
            entries_batch.append(entry)
            last_idx += 1

            if last_idx % batch_size == 0:
                try:
                    LOG.debug(f'uploading batch {begin_idx}:{last_idx}...')
                    self.request(BaseRequest('POST', f"/api/v1/data/{table_name}/batch", entries_batch))
                    populated += len(entries_batch)
                    LOG.info(f'uploaded batch {begin_idx}:{last_idx}')
                except Exception as e:
                    LOG.error(f'batch {begin_idx}:{last_idx} failed: {e}')

                begin_idx = last_idx
                entries_batch = []

        if last_idx % batch_size != 0:
            try:
                LOG.debug(f'uploading batch {begin_idx}:{last_idx}...')
                self.request(BaseRequest('POST', f"/api/v1/data/{table_name}/batch", entries_batch))
                populated += len(entries_batch)
                LOG.info(f'uploaded batch {begin_idx}:{last_idx}')
            except Exception as e:
                LOG.error(f'batch {begin_idx}:{last_idx} failed: {e}')

        if populated == 0:
            raise BaseError("failed to upload any data into Aito")

        LOG.info(f'uploaded {populated}/{last_idx} entries to table `{table_name}`')

        if optimize_on_finished:
            self.optimize_table(table_name)

    def upload_binary_file(
            self, table_name: str, binary_file: BinaryIO, optimize_on_finished: bool = True, polling_time: int = 10
    ):
        """`upload a binary file object to a table <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__

        .. note::

            requires the client to be setup with the READ-WRITE API key

        :param table_name: the name of the table
        :type table_name: str
        :param binary_file: binary file object
        :type binary_file: BinaryIO
        :param optimize_on_finished: :func:`optimize_table` when finished uploading, defaults to True
        :type optimize_on_finished: bool
        :param polling_time: polling wait time
        :type polling_time: int
        :raises RequestError: an error occurred during the upload of the file to S3
        """
        LOG.debug(f'uploading file object to table `{table_name}`...')
        LOG.debug('initiating file upload...')
        r = self.request(BaseRequest('POST', f"/api/v1/data/{table_name}/file"))
        LOG.info('file upload initiated')
        LOG.debug('getting session id and upload url...')
        upload_session_id = r['id']
        session_end_point = f'/api/v1/data/{table_name}/file/{upload_session_id}'
        s3_url = r['url']
        upload_req_method = r['method']
        LOG.info('got session id and upload url')
        LOG.debug('uploading file file to s3...')
        try:
            r = requestslib.request(upload_req_method, s3_url, data=binary_file)
            r.raise_for_status()
        except Exception as e:
            raise BaseError(f'failed to upload file to S3: {e}')
        LOG.debug('uploaded file to S3')
        LOG.debug('triggering file processing...')
        self.request(BaseRequest('POST', session_end_point))
        LOG.info('triggered file processing')
        LOG.debug('polling processing status...')
        while True:
            try:
                processing_progress = self.request(BaseRequest('GET', session_end_point))
                status = processing_progress['status']
                LOG.debug(f"completed count: {status['completedCount']}, throughput: {status['throughput']}")
                if processing_progress['errors']['message'] != 'Last 0 failing rows':
                    LOG.error(processing_progress['errors'])
                if status['finished']:
                    break
            except Exception as e:
                LOG.debug(f'failed to get file upload status: {e}')
            time.sleep(polling_time)
        LOG.info(f'uploaded file object to table `{table_name}`')
        if optimize_on_finished:
            self.optimize_table(table_name)

    def upload_file(
            self, table_name: str, file_path: PathLike, optimize_on_finished: bool = True, polling_time: int = 10):
        """`upload a file <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__ to the specfied table

        .. note::

            requires the client to be setup with the READ-WRITE API key

        :param table_name: the name of the table
        :type table_name: str
        :param file_path: path to the file to be uploaded
        :type file_path: PathLike
        :param optimize_on_finished: :func:`optimize_table` when finished uploading, defaults to True
        :type optimize_on_finished: bool
        :param polling_time: polling wait time
        :type polling_time: int
        :raises BaseError: incorrect file extension, should be .ndjson.gz
        :raises RequestError: an error occurred during the upload of the file to S3
        """
        if not check_file_is_gzipped(file_path):
            raise BaseError(f'file {file_path} is not a gzip-compressed ndjson file')
        with open(file_path, 'rb') as f:
            self.upload_binary_file(table_name, f, optimize_on_finished, polling_time)

    def create_job(self, job_endpoint: str, query: Union[List, Dict]) -> Dict:
        """Create a `job <https://aito.ai/docs/api/#post-api-v1-jobs-query>`__ for a query that takes longer than 30 seconds to run

        :param job_endpoint: job endpoint
        :type job_endpoint: str
        :param query: the query for the endpoint
        :type query: Union[List, Dict]
        :return: job information
        :rtype: Dict
        """
        return self.request(BaseRequest('POST', job_endpoint, query))

    def get_job_status(self, job_id: str) -> Dict:
        """`Get the status of a job <https://aito.ai/docs/api/#get-api-v1-jobs-uuid>`__ with the specified job id

        :param job_id: the id of the job
        :type job_id: str
        :return: job status
        :rtype: Dict
        """
        return self.request(BaseRequest(method='GET', endpoint=f'/api/v1/jobs/{job_id}'))

    def get_job_result(self, job_id: str) -> Dict:
        """`Get the result of a job <https://aito.ai/docs/api/#get-api-v1-jobs-uuid-result>`__ with the specified job id

        :param job_id: the id of the job
        :type job_id: str
        :return: the job result
        :rtype: Dict
        """
        return self.request(BaseRequest(method='GET', endpoint=f'/api/v1/jobs/{job_id}/result'))

    def job_request(
            self, job_endpoint: str, query: Union[Dict, List] = None, polling_time: int = 10
    ) -> Dict:
        """make a request to an Aito API endpoint using job

        This method should be used for requests that take longer than 30 seconds, e.g: evaluate

        The following query evaluate the perfomance of a predict query that uses the product's name to predic the product's category

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
        resp = self.create_job(job_endpoint, query)
        job_id = resp['id']
        LOG.debug('polling job status...')
        while True:
            job_status_resp = self.get_job_status(job_id)
            if 'finishedAt' in job_status_resp:
                break
            time.sleep(polling_time)
        return self.get_job_result(job_id)

    def get_table_size(self, table_name: str) -> int:
        """return the number of entries of the specified table

        :param table_name: the name of the table
        :type table_name: str
        :return: the number of entries in the table
        :rtype: int
        """
        return self.request(BaseRequest('POST', '/api/v1/_query', {'from': table_name}))['total']

    def query_entries(self, table_name: str, offset: int = 0, limit: int = 10, select: List[str] = None) -> List[Dict]:
        """`query <https://aito.ai/docs/api/#post-api-v1-query>`__ entries of the specified table

        use offset and limit for `pagination <https://aito.ai/docs/api/#pagination>`__

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

        query = {'from': table_name, 'offset': offset, 'limit': limit, 'select': select}
        return self.request(BaseRequest('POST', '/api/v1/_query', query))['hits']

    def query_all_entries(
            self,
            table_name: str,
            select: List[str] = None,
            batch_size: int = 5000
    ) -> List[Dict]:
        """`query <https://aito.ai/docs/api/#post-api-v1-query>`__  all entries of the specified table

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
        table_size = self.get_table_size(table_name)
        all_entries = []
        begin_idx = 0
        while begin_idx < table_size:
            last_idx = begin_idx + batch_size if begin_idx + batch_size <= table_size else table_size
            LOG.debug(f'getting table chunk {begin_idx}:{last_idx}...')
            all_entries += self.query_entries(table_name, offset=begin_idx, limit=batch_size, select=select)
            LOG.debug(f'queried chunk {begin_idx}:{last_idx}')
            begin_idx += batch_size
        LOG.info(f'queried all entries of table `{table_name}`')
        return all_entries

    def download_table(
            self,
            table_name: str,
            output_folder: PathLike,
            file_name: str = None,
            batch_size: int = 5000,
            gzip_output: bool = False
    ):
        """download a table to a NDJSON file or a gzipped NDJSON file

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
        table_size = self.get_table_size(table_name)
        begin_idx = 0
        while begin_idx < table_size:
            last_idx = begin_idx + batch_size if begin_idx + batch_size <= table_size else table_size
            LOG.debug(f'downloading table chunk {begin_idx}:{last_idx}...')
            hits = self.request(BaseRequest(
                'POST', '/api/v1/_query', {'from': table_name, 'offset': begin_idx, 'limit': batch_size}
            ))['hits']
            with out_file_path.open('a+') as f:
                ndjson.dump(hits, f)
                if last_idx != table_size:
                    f.write('\n')
            LOG.debug(f'downloaded table chunk {begin_idx}:{last_idx}')
            begin_idx += batch_size
        if gzip_output:
            gzip_file(out_file_path, keep=False)
        LOG.info(f'downloaded table `{table_name}` to {out_file_path}')

    def naive_predict(
            self,
            predicting_field: str,
            from_table: str,
            use_database_schema: AitoDatabaseSchema = None
    ) -> Tuple[Dict, Dict, Dict]:
        """generate an example predict query to predict a field

        The example query will use all fields of the table as the hypothesis and the first entry of the table as the input data

        :param predicting_field: the predicting field name. If the field belong to a linked table,
            it should be in the format of <column_with_link>.<field_name>
        :type predicting_field: str
        :param from_table: the name of the table the will be use as context for prediction.
            The predicting field should be "reachable" from the table (i.e: the predicting field should be a column
            in the table or a table that is linked to this table)
        :type from_table: str
        :param use_database_schema: use an existing database schema if do not want to re-fetch the database schema
        :type use_database_schema: AitoDatabaseSchema
        :return: a tuple contains the predict query and the prediction result
        :rtype: Tuple[Dict, Dict, Dict]
        """
        database_schema = use_database_schema if use_database_schema else self.get_database_schema()
        table_schema = database_schema[from_table]

        predicting_field_splitted = predicting_field.split('.')
        is_predicting_a_linked_field = len(predicting_field_splitted) == 2

        if is_predicting_a_linked_field:
            (predicting_col, linked_col) = predicting_field_splitted
            if predicting_col not in table_schema.columns:
                raise ValueError(f"table `{from_table}` does not have column `{predicting_col}`")
            linked_table = table_schema.links[predicting_col].linked_table_name
            if linked_col not in database_schema[linked_table].columns:
                raise ValueError(f"linked table `{linked_table}` does not have column `{linked_col}`")
        else:
            if predicting_field not in table_schema.columns:
                raise ValueError(f"table `{from_table}` does not have column `{predicting_field}`")
            predicting_col = predicting_field

        table_first_entry_res = self.query_entries(from_table, limit=1)
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
        actual_result = table_first_entry.get(predicting_col)
        predict_result = self.request(BaseRequest('POST', '/api/v1/_predict', predict_query))
        return predict_query, predict_result, actual_result
