import asyncio
import logging
from typing import Dict, List

import requests
from aiohttp import ClientSession


class AitoClient:
    def __init__(self, url, rw_key, ro_key):
        self.url = url
        self.rw_key = rw_key
        self.ro_key = ro_key
        self.logger = logging.getLogger("AitoClient")
        self.request_methods = {
            'PUT': requests.put, 'POST': requests.post, 'GET': requests.get, 'DELETE': requests.delete
        }
        self.query_api_paths = [
            '/api/v1/_search',
            '/api/v1/_predict',
            '/api/v1/_recommend',
            '/api/v1/_evaluate',
            '/api/v1/_similarity',
            '/api/v1/_match',
            '/api/v1/_relate',
            '/api/v1/_query'
        ]
        self.database_api_prefix_and_methods = {
            '/api/v1/schema': ['GET', 'PUT', 'DELETE'],
            '/api/v1/data': ['POST', 'GET'],
            '/api/v1/data/_delete': ['POST']
        }
        self.path_required_rw_keys = []

    def validate_path(self, path: str):
        is_database_api_path = any([path.startswith(db_path) for db_path in self.database_api_prefix_and_methods])
        if path not in self.query_api_paths or not is_database_api_path:
            raise ValueError(f"invalid path {path}")
        return path

    def build_headers(self, validated_path: str):
        headers = {'Content-Type': 'application/json'}
        if validated_path in self.query_api_paths:
            use_rw_key = False
        else:
            use_rw_key = True
        if use_rw_key and self.rw_key:
            headers['x-api-key'] = self.rw_key
        if not use_rw_key and self.ro_key:
            headers['x-api-key'] = self.ro_key
        return headers

    async def fetch(self, session: ClientSession, req_method: 'str', path: str, json_data: Dict):
        self.logger.debug(f"{req_method} to {path} with {str(json_data[:250])}")
        headers = self.build_headers(path)
        async with session.request(method=req_method, url=self.url + path, json=json_data, headers=headers) as resp:
            json_resp = await resp.json()
            self.logger.debug(f"got response ${str(json_resp)[:250]}")
            return json_resp

    def async_same_requests(self, request_count: int, request_method: str, path: str, json_data: Dict = None):
        """
        Run async n similar requests
        :param request_count: number of requests
        :param request_method:
        :param path: aito API endpoints path
        :param json_data: request content
        :return:
        """
        path = self.validate_path(path)

        async def run():
            async with ClientSession() as session:
                tasks = [self.fetch(session, request_method, path, json_data)] * request_count
                return await asyncio.gather(*tasks)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(run())

    def async_requests(self, request_methods: List[str], request_paths: List[str], request_data: List[Dict]):
        """
        Run async multiple requests
        :param request_methods: list of request methods
        :param request_paths: list of request API path
        :param request_data: list of request data
        :return:
        """
        request_paths = [self.validate_path(path) for path in request_paths]

        async def run():
            async with ClientSession() as session:
                tasks = [self.fetch(session, method, request_paths[idx], request_data[idx])
                         for idx, method in enumerate(request_methods)]
                return await asyncio.gather(*tasks)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(run())

    def request(self, req_method: str, path: str, data=None):
        path = self.validate_path(path)
        headers = self.build_headers(path)
        url = self.url + path

        logger = self.logger
        logger.debug(f"{req_method} to {path} with data {str(data)[:250]}...")
        r = self.request_methods[req_method](url, headers=headers, json=data)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            raise Exception(f"Unsuccessful request: {r.content}")
        return r.json()

    def put_database_schema(self, database_schema):
        return self.request('PUT', '/api/v1/schema', database_schema)

    def get_database_schema(self):
        return self.request('GET', '/api/v1/schema')

    def delete_database(self):
        return self.request('DELETE', '/api/v1/schema')

    def put_table_schema(self, table_name, table_schema):
        return self.request('PUT', f"/api/v1/schema/{table_name}", table_schema)

    def delete_table(self, table_name):
        return self.request('DELETE', f"/api/v1/schema/{table_name}")

    def get_table_schema(self, table_name):
        return self.request('GET', f"/api/v1/schema/{table_name}")

    def populate_table_entries(self, table_name, entries):
        self.logger.info(f"Start uploading {len(entries)} to table {table_name}")
        resp = self.request('POST', f"/api/v1/data/{table_name}/batch", entries)
        self.logger.info(f"Finish uploading {len(entries)} to table {table_name}")
        return resp

    def populate_table_entries_by_batches(self, table_name, entries, batch_size=1000):
        self.logger.info(f"Start uploading {len(entries)} to table {table_name} with batch size of {batch_size}")
        begin_idx = 0
        populated = 0
        while begin_idx < len(entries):
            last = begin_idx + batch_size
            entries_batch = entries[begin_idx:last]
            try:
                self.populate_table_entries(table_name, entries_batch)
                populated += len(entries_batch)
            except Exception as e:
                self.logger.error(f"Batch {begin_idx}:{last} failed: {e}")
            begin_idx = last

        self.logger.info(f"Finish uploading {populated}/{len(entries)} entries to table {table_name} "
                         f"with batch size of {batch_size}")