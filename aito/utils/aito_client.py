import asyncio
import logging
import time
from timeit import default_timer
from typing import Dict, List, BinaryIO, Union, Tuple

import requests
from aiohttp import ClientSession, ClientResponseError

LOG = logging.getLogger('AitoClient')


class BaseError(Exception):
    def __init__(self, message):
        LOG.error(message)
        LOG.debug(message, stack_info=True)
        self.message = message

    def __str__(self):
        return f'AitoClientError: {self.message}'


class RequestError(BaseError):
    def __init__(self, method: str, endpoint: str, query: Union[List, Dict], error: Exception):
        if isinstance(error, requests.HTTPError):
            error_msg = error.response.json()['message']
        elif isinstance(error, ClientResponseError):
            error_msg = error.message
        else:
            error_msg = str(error)
        super().__init__(f'failed to {method} to {endpoint} with query {query}) {error_msg}')


class AitoClient:
    """A versatile client for your Aito Database Instance

    :ivar request_methods: request methods
    :ivar query_endpoints: recognized query API endpoints
    :ivar database_endpoints: recognized database API endpoints
    """
    _request_methods = {
        'PUT': requests.put, 'POST': requests.post, 'GET': requests.get, 'DELETE': requests.delete
    }
    _query_paths = ['_search', '_predict', '_recommend', '_evaluate', '_similarity', '_match', '_relate', '_query']
    _query_endpoints = [f'/api/v1/{p}' for p in _query_paths] + ['/version']
    _database_endpoints = ['/api/v1/schema', '/api/v1/data']
    _job_endpoint = '/api/v1/jobs'

    def __init__(self, instance_url: str, api_key: str, check_credentials: bool = True):
        """Constructor method
        :param instance_url: Aito instance url
        :type instance_url: str
        :param api_key: Aito instance API key
        :type api_key: str
        :param check_credentials: Check the given credentials by requesting Aito instance version, default to True
        :type check_credentials: bool
        :raises AitoClientError: An error occurred during the creation of AitoClient
        """
        self.url = instance_url
        self.headers = {'Content-Type': 'application/json', 'x-api-key': api_key}
        if check_credentials:
            try:
                self.get_version()
            except Exception:
                raise BaseError('failed to instantiate Aito Client, please check your credentials')

    def _check_endpoint(self, endpoint: str):
        """raise error if erroneous endpoint and warn if the unrecognied endpoint

        :param endpoint: endpoint
        :return:
        """
        if not endpoint.startswith('/'):
            raise BaseError('endpoint must start with `/` character')
        is_database_path = any([endpoint.startswith(db_endpoint) for db_endpoint in self._database_endpoints])
        is_job_path = endpoint.startswith(self._job_endpoint)
        if not is_database_path and not is_job_path and endpoint not in self._query_endpoints:
            LOG.warning(f'unrecognized endpoint {endpoint}')

    async def _async_request(
            self, session: ClientSession, method: str, endpoint: str, query: Union[List, Dict]
    ) -> Tuple[int, Union[Dict, List]]:
        """async a request

        :param session: aiohttp client session
        :param method: request method
        :param endpoint: request endpoint
        :param query: request query
        :return: tuple of request status code and request json content
        """
        self._check_endpoint(endpoint)
        LOG.debug(f'async {method} to {endpoint} with query {query}')
        async with session.request(method=method, url=self.url + endpoint, json=query, headers=self.headers) as resp:
            return resp.status, await resp.json()

    async def _bounded_async_request(self, semaphore: asyncio.Semaphore, *args) -> Tuple[int, Union[Dict, List]]:
        """bounded requests by semaphore

        :param semaphore: asyncio Semaphore
        :param args: request args
        :return:
        """
        async with semaphore:
            return await self._async_request(*args)

    def async_requests(
            self,
            methods: List[str],
            endpoints: List[str],
            queries: List[Union[List, Dict]],
            batch_size: int = 10
    ) -> List[Dict]:
        """async multiple requests

        This method is useful when sending a batch of, for example, predict requests.

        :param methods: list of request methods
        :type methods: List[str]
        :param endpoints: list of request endpoints
        :type endpoints: List[str]
        :param queries: list of request queries
        :type queries: List[Dict]
        :param batch_size: number of queries to be sent at a time
        :type batch_size: int
        :return: list of request response or exception if request did not succeed
        :rtype: List[Dict]
        """
        async def run():
            async with ClientSession() as session:
                tasks = [
                    self._bounded_async_request(semaphore, session, method, endpoints[idx], queries[idx])
                    for idx, method in enumerate(methods)
                ]
                return await asyncio.gather(*tasks, return_exceptions=True)

        semaphore = asyncio.Semaphore(batch_size)
        loop = asyncio.get_event_loop()
        responses = loop.run_until_complete(run())
        for resp_idx, resp in enumerate(responses):
            if isinstance(resp, Exception):
                responses[resp_idx] = RequestError(methods[resp_idx], endpoints[resp_idx], queries[resp_idx], resp)
            # manually handling raise for status since aiohttp doesnt return response content with raise for status
            elif resp[0] >= 400:
                responses[resp_idx] = RequestError(methods[resp_idx], endpoints[resp_idx], queries[resp_idx], resp[1])
            else:
                responses[resp_idx] = resp[1]
        return responses

    def request(self, method: str, endpoint: str, query: Union[Dict, List] = None) -> Dict:
        """Make a request to an Aito API endpoint

        :param method: request method
        :type method: str
        :param endpoint: an Aito API endpoint
        :type endpoint: str
        :param query: an Aito query, defaults to None
        :type query: Union[Dict, List], optional
        :raises AitoClientRequestError: An error occurred during request
        :return: request JSON content if succeed
        :rtype: Dict
        """
        self._check_endpoint(endpoint)
        url = self.url + endpoint
        try:
            resp = self._request_methods[method](url, headers=self.headers, json=query)
            resp.raise_for_status()
            json_resp = resp.json()
        except Exception as e:
            raise RequestError(method, endpoint, query, e)
        return json_resp

    def get_version(self) -> Dict:
        """get the aito instance version

        :return: version information in json format
        :rtype: Dict
        """
        return self.request('GET', '/version')

    def put_database_schema(self, database_schema: Dict) -> Dict:
        """create database with the given database schema `API doc <https://aito.ai/docs/api/#put-api-v1-schema>`__

        :param database_schema: Aito database schema
        :type database_schema: Dict
        :return: the database schema if successful
        :rtype: Dict
        """
        r = self.request('PUT', '/api/v1/schema', database_schema)
        LOG.info('database schema created')
        return r

    def get_database_schema(self) -> Dict:
        """get the schema of the database `API doc <https://aito.ai/docs/api/#get-api-v1-schema>`__

        :return: Aito database schema if successful
        :rtype: Dict
        """
        return self.request('GET', '/api/v1/schema')

    def delete_database(self) -> Dict:
        """delete the whole database `API doc <https://aito.ai/docs/api/#delete-api-v1-schema>`__

        :return: deleted tables
        :rtype: Dict
        """
        r = self.request('DELETE', '/api/v1/schema')
        LOG.info('database deleted')
        return r

    def put_table_schema(self, table_name: str, table_schema: Dict) -> Dict:
        """create a table with the given table schema `API doc <https://aito.ai/docs/api/#put-api-v1-schema-table>`__

        :param table_name: the name of the table
        :type table_name: str
        :param table_schema: Aito table schema
        :type table_schema: Dict
        :return: the table schema if successful
        :rtype: Dict
        """
        r = self.request('PUT', f'/api/v1/schema/{table_name}', table_schema)
        LOG.info(f'table `{table_name}` created')
        return r

    def get_table_schema(self, table_name: str) -> Dict:
        """get a table schema

        :param table_name: the name of the table
        :type table_name: str
        :return: the table schema if successful
        :rtype: Dict
        """
        return self.request('GET', f'/api/v1/schema/{table_name}')

    def delete_table(self, table_name: str) -> Dict:
        """delete a table `API doc <https://aito.ai/docs/api/#delete-api-v1-schema>`__

        :param table_name: the name of the table
        :type table_name: str
        :return: deleted table if successful
        :rtype: Dict
        """
        r = self.request('DELETE', f'/api/v1/schema/{table_name}')
        LOG.info(f'table `{table_name}` deleted')
        return r

    def get_existing_tables(self) -> List[str]:
        """get a list of existing tables in the instance

        :return: list of the names of existing tables
        :rtype: List[str]
        """
        return list(self.get_database_schema()['schema'].keys())

    def check_table_exists(self, table_name: str) -> bool:
        """check if a table exist in the instance

        :param table_name: the name of the table
        :type table_name: str
        :return: True if the table exists
        :rtype: bool
        """
        return table_name in self.get_existing_tables()

    def populate_table_entries(self, table_name: str, entries: List[Dict]):
        """populate a list of entries to a table API doc `API doc <https://aito.ai/docs/api/#post-api-v1-data-table-batch>`__

        if the list contains more than 1000 entries, upload the entries by batch of 1000

        :param table_name: the name of the table
        :type table_name: str
        :param entries: list of the table entries
        :type entries: List[Dict]
        """
        if len(entries) > 1000:
            self.populate_table_entries_by_batches(table_name, entries)
        else:
            LOG.debug(f'uploading {len(entries)} to table `{table_name}`...')
            self.request('POST', f"/api/v1/data/{table_name}/batch", entries)
            LOG.info(f'uploaded {len(entries)} entries to table `{table_name}`')

    def populate_table_entries_by_batches(self, table_name: str, entries: List[Dict], batch_size: int = 1000):
        """populate a list of table entries by batches

        :param table_name: the name of the table
        :type table_name: str
        :param entries: list of the table entries
        :type entries: List[Dict]
        :param batch_size: the batch size, defaults to 1000
        :type batch_size: int, optional
        """
        _l = LOG
        LOG.debug(f'uploading {len(entries)} entries to table `{table_name}` with batch size of {batch_size}...')
        begin_idx = 0
        populated = 0
        while begin_idx < len(entries):
            last_idx = begin_idx + batch_size
            if last_idx > len(entries):
                last_idx = len(entries)
            entries_batch = entries[begin_idx:last_idx]
            try:
                LOG.debug(f'uploading batch {begin_idx}:{last_idx}...')
                self.populate_table_entries(table_name, entries_batch)
                populated += len(entries_batch)
                LOG.info(f'uploaded batch {begin_idx}:{last_idx}')
            except Exception as e:
                LOG.error(f'batch {begin_idx}:{last_idx} failed: {e}')
            begin_idx = last_idx

        LOG.info(f'uploaded {populated}/{len(entries)} entries to table `{table_name}`')

    def populate_table_by_file_upload(self, table_name: str, binary_file_object: BinaryIO, polling_time: int = 10):
        """upload binary file object to a table `API doc <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__

        :param table_name: the name of the table
        :type table_name: str
        :param binary_file_object: binary file object
        :type binary_file_object: BinaryIO
        :param polling_time: polling wait time
        :raises AitoClientRequestError: An error occurred during the upload of the file to S3
        """
        _l = LOG
        LOG.debug(f'uploading file object to table `{table_name}`...')
        LOG.debug('initiating file upload...')
        r = self.request('POST', f"/api/v1/data/{table_name}/file")
        LOG.info('file upload initiated')
        LOG.debug('getting session id and upload url...')
        upload_session_id = r['id']
        session_end_point = f'/api/v1/data/{table_name}/file/{upload_session_id}'
        s3_url = r['url']
        upload_req_method = r['method']
        LOG.info('got session id and upload url')
        LOG.debug('uploading file file to s3...')
        try:
            r = requests.request(upload_req_method, s3_url, data=binary_file_object)
            r.raise_for_status()
        except Exception as e:
            raise BaseError(f'fail to upload file to S3: {e}')
        LOG.debug('uploaded file to S3')
        LOG.debug('trigger file processing...')
        self.request('POST', session_end_point)
        LOG.info('triggered file processing')
        LOG.debug('polling processing status...')
        while True:
            try:
                processing_progress = self.request('GET', session_end_point)
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

    def query_table_entries(self, table_name: str, offset: int = 0, limit: int = 10) -> Dict:
        """query entries of a table `API doc <https://aito.ai/docs/api/#post-api-v1-query>`__

        use offset and limit for `pagination <https://aito.ai/docs/api/#pagination>`__

        :param table_name: the name of the table
        :type table_name: str
        :param offset: offset, defaults to 0
        :type offset: int, optional
        :param limit: limit, defaults to 10
        :type limit: int, optional
        :return: the table entries if successful
        :rtype: Dict
        """

        query = {'from': table_name, 'offset': offset, 'limit': limit}
        return self.request('POST', '/api/v1/_query', query)

    def create_job(self, job_endpoint: str, query: Union[List, Dict]) -> Dict:
        """Create a job for queries that takes longer than 30 seconds to run

        `API doc <https://aito.ai/docs/api/#post-api-v1-jobs-query>`__
        :param job_endpoint: job endpoint
        :type job_endpoint: str
        :param query: the query for the endpoint
        :type query: Union[List, Dict]
        :return: job information
        :rtype: Dict
        """
        return self.request('POST', job_endpoint, query)

    def get_job_status(self, job_id: str) -> Dict:
        """Get the status of a job with the given id

        `API doc <https://aito.ai/docs/api/#get-api-v1-jobs-uuid>`__
        :param job_id: the id of the job
        :type job_id: str
        :return: job status
        :rtype: Dict
        """
        return self.request(method='GET', endpoint=f'/api/v1/jobs/{job_id}')

    def get_job_result(self, job_id: str) -> Dict:
        """Get the result of a job with the given id

        `API doc <https://aito.ai/docs/api/#get-api-v1-jobs-uuid-result>`__
        :param job_id: the id of the job
        :type job_id: str
        :return: the job result
        :rtype: Dict
        """
        return self.request(method='GET', endpoint=f'/api/v1/jobs/{job_id}/result')

    def job_request(
            self, job_endpoint: str, query: Union[Dict, List] = None, polling_time: int = 10
    ) -> Dict:
        """Make a request to an Aito API endpoint using job

        This method should be used for request that takes longer than 30 seconds, e.g: evaluate

        :param job_endpoint: job end point
        :type job_endpoint: str
        :param query: an Aito query, defaults to None
        :type query: Union[Dict, List], optional
        :param polling_time: polling wait time, default to 10
        :type polling_time: int
        :raises AitoClientRequestError: An error occurred during request
        :return: request JSON content if succeed
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