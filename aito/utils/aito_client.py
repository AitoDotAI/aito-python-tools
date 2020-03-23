import asyncio
import logging
import re
import time
from timeit import default_timer
from typing import Dict, List, BinaryIO, Union

import requests
from aiohttp import ClientSession

LOG = logging.getLogger('AitoClient')


class AitoClientError(Exception):
    def __init__(self, message):
        super().__init__(message)
        LOG.error(message)
        self.message = message

    def __str__(self):
        return f'AitoClientError: {self.message}'


class AitoClientRequestError(AitoClientError):
    def __str__(self):
        return f'AitoClient failed to make request {self.message}'


class AitoClient:
    """A versatile client for your Aito Database Instance

    :ivar request_methods: request methods
    :ivar query_endpoints: recognized query API endpoints
    :ivar database_endpoints: recognized database API endpoints
    """
    request_methods = {
        'PUT': requests.put, 'POST': requests.post, 'GET': requests.get, 'DELETE': requests.delete
    }

    query_endpoints = [
        '/api/v1/_search',
        '/api/v1/_predict',
        '/api/v1/_recommend',
        '/api/v1/_evaluate',
        '/api/v1/_similarity',
        '/api/v1/_match',
        '/api/v1/_relate',
        '/api/v1/_query',
        '/version'
    ]

    database_endpoints = [
        '/api/v1/schema',
        '/api/v1/data'
    ]

    def __init__(self, instance_url: str, api_key: str):
        """Constructor method
        :param instance_url: Aito instance url
        :type instance_url: str
        :param api_key: Aito instance API key
        :type api_key: str
        :raises AitoClientError: An error occurred during the creation of AitoClient
        """
        self.url = instance_url
        self.headers = {'Content-Type': 'application/json', 'x-api-key': api_key}

    def _check_endpoint(self, endpoint: str):
        is_database_path = any([endpoint.startswith(db_path) for db_path in self.database_endpoints])
        if not is_database_path and endpoint not in self.query_endpoints:
            LOG.warning(f'unrecognized endpoint {endpoint}')

    async def _async_fetch(self, session: ClientSession, method: str, path: str, json_data: Dict):
        self._check_endpoint(path)
        LOG.debug(f'async {method} {path} {json_data}')
        async with session.request(method=method, url=self.url + path, json=json_data, headers=self.headers) as resp:
            json_resp = await resp.json()
            return json_resp

    def async_requests(self, methods: List[str], endpoints: List[str], queries: List[Dict]) -> List[Dict]:
        """async multiple requests

        This method is useful when sending a batch of, for example, predict requests.

        :param methods: list of request methods
        :type methods: List[str]
        :param endpoints: list of request endpoints
        :type endpoints: List[str]
        :param queries: list of request queries
        :type queries: List[Dict]
        :return: list of responses
        :rtype: List[Dict]
        """
        async def run():
            async with ClientSession() as session:
                tasks = [self._async_fetch(session, method, endpoints[idx], queries[idx])
                         for idx, method in enumerate(methods)]
                return await asyncio.gather(*tasks)

        request_count = len(methods)
        LOG.debug(f'async {request_count} requests')
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        responses = loop.run_until_complete(run())
        LOG.debug(f'responses: {responses}')
        LOG.info(f'requested {request_count} requests')
        return responses

    def request(self, req_method: str, endpoint: str, query: Union[Dict, List] = None) -> Dict:
        """Make a request to an Aito API endpoint

        :param req_method: request method
        :type req_method: str
        :param endpoint: an Aito API endpoint
        :type endpoint: str
        :param query: an Aito query, defaults to None
        :type query: Union[Dict, List], optional
        :raises AitoClientRequestError: An error occurred during request
        :return: request JSON content if succeed
        :rtype: Dict
        """
        self._check_endpoint(endpoint)
        LOG.debug(f'`{req_method}` to `{endpoint}` with query: {query}')
        url = self.url + endpoint
        try:
            resp = self.request_methods[req_method](url, headers=self.headers, json=query)
        except Exception as e:
            raise AitoClientRequestError(e)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise AitoClientRequestError(f'{e} {resp.text}')
        try:
            LOG.debug(f'response content : {resp.content}')
            json_resp = resp.json()
        except Exception as e:
            raise AitoClientRequestError(f'unknown error: {e}')
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

    def populate_table_by_file_upload(self, table_name: str, binary_file_object: BinaryIO):
        """upload binary file object to a table `API doc <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__

        :param table_name: the name of the table
        :type table_name: str
        :param binary_file_object: binary file object
        :type binary_file_object: BinaryIO
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
            raise AitoClientRequestError(f'fail to upload file to S3: {e}')
        LOG.debug('uploaded file to S3')
        LOG.debug('trigger file processing...')
        self.request('POST', session_end_point)
        LOG.info('triggered file processing')
        start_polling = default_timer()
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
            time_elapsed = default_timer() - start_polling
            if time_elapsed < 60:
                sleeping_time = 5
            elif time_elapsed < 180:
                sleeping_time = 15
            elif time_elapsed < 300:
                sleeping_time = 30
            else:
                sleeping_time = 60
            time.sleep(sleeping_time)

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
