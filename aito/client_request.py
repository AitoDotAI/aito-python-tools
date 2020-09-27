"""Request objects that is sent by an :class:`~aito.client.AitoClient` to an Aito instance

"""

import logging
from typing import Optional, Union, Dict, List

LOG = logging.getLogger('AitoClientRequest')


class BaseRequest:
    """Base request to the Aito instance"""
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
        """

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

    def _is_same_type(self, req: 'BaseRequest'):
        """check if another request object is of the same type by comaring method and endpoint"""
        return self.method == req.method and self.endpoint == req.endpoint


class SearchRequest(BaseRequest):
    """Request of the `Search query <https://aito.ai/docs/api/#post-api-v1-search>`__"""
    def __init__(self, query: Dict):
        super().__init__('POST', '/api/v1/_search', query)


class PredictRequest(BaseRequest):
    """Request of the `Predict query <https://aito.ai/docs/api/#post-api-v1-predict>`__"""
    def __init__(self, query: Dict):
        super().__init__('POST', '/api/v1/_predict', query)


class RecommendRequest(BaseRequest):
    """Request of the `Recommend query <https://aito.ai/docs/api/#post-api-v1-recommend>`__"""
    def __init__(self, query: Dict):
        super().__init__('POST', '/api/v1/_recommend', query)


class EvaluateRequest(BaseRequest):
    """Request of the `Evaluate query <https://aito.ai/docs/api/#post-api-v1-evaluate>`__"""
    def __init__(self, query: Dict):
        super().__init__('POST', '/api/v1/_evaluate', query)


class SimilarityRequest(BaseRequest):
    """Request of the `Similarity query <https://aito.ai/docs/api/#post-api-v1-similarity>`__"""
    def __init__(self, query: Dict):
        super().__init__('POST', '/api/v1/_similarity', query)


class MatchRequest(BaseRequest):
    """Request of the `Match query <https://aito.ai/docs/api/#post-api-v1-match>`__"""
    def __init__(self, query: Dict):
        super().__init__('POST', '/api/v1/_match', query)


class RelateRequest(BaseRequest):
    """Request of the `Relate query <https://aito.ai/docs/api/#post-api-v1-relate>`__"""
    def __init__(self, query: Dict):
        super().__init__('POST', '/api/v1/_relate', query)


class GenericQueryRequest(BaseRequest):
    """Response of the `Generic query <https://aito.ai/docs/api/#post-api-v1-query>`__"""
    def __init__(self, query: Dict):
        super().__init__('POST', '/api/v1/_query', query)