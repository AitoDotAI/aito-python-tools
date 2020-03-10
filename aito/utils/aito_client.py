import asyncio
import logging
import re
import time
from timeit import default_timer
from typing import Dict, List, BinaryIO

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
    request_methods = {
        'PUT': requests.put, 'POST': requests.post, 'GET': requests.get, 'DELETE': requests.delete
    }

    paths = [
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

    database_paths_prefix = [
        '/api/v1/schema',
        '/api/v1/data'
    ]

    def __init__(self, instance_name: str, api_key: str):
        if not instance_name:
            raise AitoClientError('instance name is required')
        instance_name = instance_name.strip()
        if not re.fullmatch('^[a-z][a-z0-9-]*', instance_name):
            raise AitoClientError(
                'instance name must begin with a letter and can only contains lowercase letters, numbers, and dashses'
            )
        self.url = f'https://{instance_name}.api.aito.ai'
        if not api_key:
            raise AitoClientError('instance API key is required')
        self.headers = {'Content-Type': 'application/json', 'x-api-key': api_key}

    def _check_endpoint(self, endpoint: str):
        is_database_path = any([endpoint.startswith(db_path) for db_path in self.database_paths_prefix])
        if not is_database_path and endpoint not in self.paths:
            LOG.warning(f'unrecognized endpoint {endpoint}')

    async def _async_fetch(self, session: ClientSession, method: str, path: str, json_data: Dict):
        self._check_endpoint(path)
        LOG.debug(f'async {method} {path} {json_data}')
        async with session.request(method=method, url=self.url + path, json=json_data, headers=self.headers) as resp:
            json_resp = await resp.json()
            return json_resp

    def async_similar_requests(self, request_count: int, request_method: str, path: str, json_data: Dict = None):
        """ async multiple similar requests

        :param request_count: number of requests
        :param request_method:
        :param path: aito API endpoints path
        :param json_data: request content
        :return:
        """
        async def run():
            async with ClientSession() as session:
                tasks = [self._async_fetch(session, request_method, path, json_data)] * request_count
                return await asyncio.gather(*tasks)

        LOG.debug(f'async {request_count} similar requests')
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        responses = loop.run_until_complete(run())
        LOG.debug(f'responses: {responses}')
        LOG.info(f'requested {request_count} similar requests')
        return responses

    def async_requests(self, request_methods: List[str], request_paths: List[str], request_data: List[Dict]):
        """ async multiple requests

        :param request_methods: list of request methods
        :param request_paths: list of request API path
        :param request_data: list of request data
        :return:
        """
        async def run():
            async with ClientSession() as session:
                tasks = [self._async_fetch(session, method, request_paths[idx], request_data[idx])
                         for idx, method in enumerate(request_methods)]
                return await asyncio.gather(*tasks)

        request_count = len(request_methods)
        LOG.debug(f'async {request_count} requests')
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        responses = loop.run_until_complete(run())
        LOG.debug(f'responses: {responses}')
        LOG.info(f'requested {request_count} requests')
        return responses

    def request(self, req_method: str, path: str, query: Dict =None):
        """ request to an aito API end point

        :param req_method: request method
        :param path: an Aito API endpoint
        :param query: an Aito query
        :return:
        """
        self._check_endpoint(path)
        LOG.debug(f'`{req_method}` to `{path}` with query: {query}')
        url = self.url + path
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

    def get_version(self):
        return self.request('GET', '/version')

    def put_database_schema(self, database_schema):
        r = self.request('PUT', '/api/v1/schema', database_schema)
        LOG.info('database schema created')
        return r

    def get_database_schema(self):
        return self.request('GET', '/api/v1/schema')

    def delete_database(self):
        r = self.request('DELETE', '/api/v1/schema')
        LOG.info('database deleted')
        return r

    def put_table_schema(self, table_name, table_schema):
        r = self.request('PUT', f'/api/v1/schema/{table_name}', table_schema)
        LOG.info(f'table `{table_name}` created')
        return r

    def delete_table(self, table_name):
        r = self.request('DELETE', f'/api/v1/schema/{table_name}')
        LOG.info(f'table `{table_name}` deleted')
        return r

    def get_table_schema(self, table_name):
        return self.request('GET', f'/api/v1/schema/{table_name}')

    def get_existing_tables(self) -> List[str]:
        return list(self.get_database_schema()['schema'].keys())

    def check_table_existed(self, table_name) -> bool:
        return table_name in self.get_existing_tables()

    def populate_table_entries(self, table_name, entries):
        if len(entries) > 1000:
            self.populate_table_entries_by_batches(table_name, entries)
        else:
            LOG.debug(f'uploading {len(entries)} to table `{table_name}`...')
            self.request('POST', f"/api/v1/data/{table_name}/batch", entries)
            LOG.info(f'uploaded {len(entries)} entries to table `{table_name}`')

    def populate_table_entries_by_batches(self, table_name, entries, batch_size=1000):
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

    def query_table_entries(self, table_name: str, limit: int = None):
        query = {'from': table_name}
        if limit:
            query['limit'] = limit
        return self.request('POST', '/api/v1/_query', query)
