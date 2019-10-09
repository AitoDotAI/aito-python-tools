import asyncio
import logging
from pathlib import Path
from typing import Dict, List
import time
from timeit import default_timer

import requests
from aiohttp import ClientSession


class AitoClient:
    def __init__(self, url: str, rw_key: str, ro_key: str):
        self.logger = logging.getLogger("AitoClient")
        if not url:
            raise Exception(f"Client url is not defined")
        self.url = url

        self.rw_key = rw_key
        self.ro_key = ro_key
        if not rw_key:
            self.logger.warn("Read-write key is not defined. API required read-write key will fail")
        if not ro_key:
            self.logger.warn(f"Read-only key is not defined. Use read-write key instead")
            self.ro_key = self.rw_key

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
        self.ro_key_paths = self.query_api_paths + ['/version']

    def build_headers(self, path: str):
        """
        Check if path is validate aito path.
        Use read-write or read-only key accordingly
        :param path:
        :return:
        """
        headers = {'Content-Type': 'application/json'}
        is_database_api_path = any([path.startswith(db_path) for db_path in self.database_api_prefix_and_methods])
        if is_database_api_path:
            headers['x-api-key'] = self.rw_key
        elif path in self.ro_key_paths:
            headers['x-api-key'] = self.ro_key
        else:
            raise ValueError(f"invalid path {path}")
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
        async def run():
            async with ClientSession() as session:
                tasks = [self.fetch(session, method, request_paths[idx], request_data[idx])
                         for idx, method in enumerate(request_methods)]
                return await asyncio.gather(*tasks)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(run())

    def request(self, req_method: str, path: str, data=None):
        headers = self.build_headers(path)
        url = self.url + path

        logger = self.logger
        logger.debug(f"{req_method} to {path} with data {str(data)[:250]}...")
        r = self.request_methods[req_method](url, headers=headers, json=data)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            raise Exception(f"Unsuccessful request: {r.status_code} {r.content}")
        return r.json()

    def get_version(self):
        return self.request('GET', '/version')

    def put_database_schema(self, database_schema):
        self.logger.info("Creating database schema...")
        r = self.request('PUT', '/api/v1/schema', database_schema)
        self.logger.info("Database schema created")
        return r

    def get_database_schema(self):
        return self.request('GET', '/api/v1/schema')

    def delete_database(self):
        self.logger.info("Deleting the whole database...")
        r = self.request('DELETE', '/api/v1/schema')
        self.logger.info("Database deleted")
        return r

    def put_table_schema(self, table_name, table_schema):
        self.logger.info(f"Creating table '{table_name}' schema...")
        r = self.request('PUT', f"/api/v1/schema/{table_name}", table_schema)
        self.logger.info(f"Table '{table_name}' schema created")
        return r

    def delete_table(self, table_name):
        self.logger.info(f"Deleting table '{table_name}'")
        r =  self.request('DELETE', f"/api/v1/schema/{table_name}")
        self.logger.info(f"Table '{table_name}' deleted")
        return r

    def get_table_schema(self, table_name):
        return self.request('GET', f"/api/v1/schema/{table_name}")

    def get_existing_tables(self) -> List[str]:
        return list(self.get_database_schema()['schema'].keys())

    def check_table_existed(self, table_name) -> bool:
        return table_name in self.get_existing_tables()

    def populate_table_entries(self, table_name, entries):
        if not self.check_table_existed(table_name):
            raise ValueError(f"Table '{table_name}' does not exist. Please create the table schema first")
        if len(entries) > 1000:
            self.populate_table_entries_by_batches(table_name, entries)
        else:
            self.logger.info(f"Start uploading {len(entries)} entries to table '{table_name}'...")
            self.request('POST', f"/api/v1/data/{table_name}/batch", entries)
            self.logger.info(f"Uploaded {len(entries)} entries to table '{table_name}'")

    def populate_table_entries_by_batches(self, table_name, entries, batch_size=1000):
        if not self.check_table_existed(table_name):
            raise ValueError(f"Table '{table_name}' does not exist. Please create the table schema first")
        self.logger.info(f"Start uploading {len(entries)} entries to table '{table_name}' "
                         f"with batch size of {batch_size}...")
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

        self.logger.info(f"Uploaded {populated}/{len(entries)} entries to table '{table_name}'")

    def populate_table_by_file_upload(self, table_name: str, file_path: Path):
        if not self.check_table_existed(table_name):
            raise ValueError(f"Table '{table_name}' does not exist. Please create the table schema first")
        if file_path.suffixes[-2:] != ['.ndjson', '.gz']:
            raise ValueError("Uploading file must be in gzip compressed ndjson format")
        self.logger.info("Initiating file upload...")
        r = self.request('POST', f"/api/v1/data/{table_name}/file")
        upload_session_id = r['id']
        session_end_point = f'/api/v1/data/{table_name}/file/{upload_session_id}'
        s3_url = r['url']
        upload_req_method = r['method']

        self.logger.info("Uploading file to S3...")
        try:
            requests.request(upload_req_method, s3_url, data=file_path.open(mode='rb'))
            self.logger.info("Uploading file to S3 completed")
        except Exception as e:
            self.logger.error(f"Failed to upload file to S3: {e}")
            raise e
        self.logger.info("Triggering file processing...")
        self.request('POST', session_end_point)

        start_polling = default_timer()
        while True:
            processing_progress = self.request('GET', session_end_point)
            status = processing_progress['status']
            self.logger.info(f"completed count: {status['completedCount']}, throughput: {status['throughput']}")
            if processing_progress['errors']['message'] != 'Last 0 failing rows':
                self.logger.error(processing_progress['error'])
            if status['finished']:
                break
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

        self.logger.info(f"Populate table '{table_name}' by file upload completed")

    def query_table_entries(self, table_name: str, limit: int = None):
        if table_name not in self.get_existing_tables():
            raise ValueError(f"Table {table_name} does not exist")
        query = {'from': table_name}
        if limit:
            query['limit'] = limit
        return self.request('POST', '/api/v1/_query', query)




