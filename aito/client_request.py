import logging
from typing import Optional, Union, Dict, List

LOG = logging.getLogger('AitoClientRequest')


class BaseRequest:
    _query_paths = ['_search', '_predict', '_recommend', '_evaluate', '_similarity', '_match', '_relate', '_query']
    _query_endpoints = [f'/api/v1/{p}' for p in _query_paths] + ['/version']
    _database_endpoints = ['/api/v1/schema', '/api/v1/data']
    _job_endpoint = '/api/v1/jobs'
    _request_methods = ['PUT', 'POST', 'GET', 'DELETE']

    def _check_endpoint(self, endpoint: str):
        """raise error if erroneous endpoint and warn if the unrecognized endpoint, else return the endpoint
        """
        if not endpoint.startswith('/'):
            raise ValueError('endpoint must start with the `/` character')
        is_database_path = any([endpoint.startswith(db_endpoint) for db_endpoint in self._database_endpoints])
        is_job_path = endpoint.startswith(self._job_endpoint)
        if not is_database_path and not is_job_path and endpoint not in self._query_endpoints:
            LOG.warning(f'unrecognized endpoint {endpoint}')
        return endpoint

    def _check_method(self, method: str):
        """raise error if incorrect method else return the method
        """
        method = method.upper()
        if method not in self._request_methods:
            raise ValueError(
                f"incorrect request method `{method}`. Method must be one of {'|'.join(self._request_methods)}"
            )
        return method

    def __init__(self, method: str, endpoint: str, query: Optional[Union[Dict, List]] = None):
        """ A request object to be sent to an Aito instance

        :param method: request method
        :type method: str
        :param endpoint: request endpoint
        :type endpoint: str
        :param query: an Aito query if applicable, optional
        :type query: Optional[Union[Dict, List]]
        """
        self.method = self._check_method(method)
        self.endpoint = self._check_endpoint(endpoint)
        self.query = query

    def __str__(self):
        return f'{self.method}({self.endpoint}): {str(self.query)[:100]}'