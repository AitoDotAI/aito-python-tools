from os import getenv

from aito.client import AitoClient
from aito.client.requests import SearchRequest, PredictRequest, RecommendRequest, EvaluateRequest, \
    SimilarityRequest, MatchRequest, RelateRequest, GenericQueryRequest
from aito.client.responses import HitsResponse, SearchResponse, PredictResponse, RecommendResponse, \
    SimilarityResponse, MatchResponse, RelateResponse, EvaluateResponse


def get_env_var(var_name):
    env_var = getenv(var_name)
    if not env_var:
        raise KeyError(f'environment variable `{var_name}` not found')
    return env_var


def default_client():
    return AitoClient(get_env_var('AITO_INSTANCE_URL'), get_env_var('AITO_API_KEY'))


def grocery_demo_client():
    return AitoClient(get_env_var('AITO_GROCERY_DEMO_INSTANCE_URL'), get_env_var('AITO_GROCERY_DEMO_API_KEY'))


# test context for each endpoint consists of: endpoint, request_cls, query, response_cls
endpoint_methods_test_context = [
    ('search', SearchRequest, {"from": "users"}, SearchResponse),
    (
            'predict',
            PredictRequest,
            {"from": "products", "where": {"name": "Pirkka banana"}, "predict": "tags"},
            PredictResponse
    ),
    (
            'recommend',
            RecommendRequest,
            {"from": "impressions", "recommend": "product", "goal": {"context.user": "veronica"}},
            RecommendResponse
    ),
    (
            'evaluate',
            EvaluateRequest,
            {
                "test": {"$index": {"$mod": [10, 0]}},
                "evaluate": {
                    "from": "products",
                    "where": {"name": {"$get": "name"}},
                    "match": "tags"
                }
            },
            EvaluateResponse
    ),
    (
            'similarity',
            SimilarityRequest,
            {"from": "products", "similarity": {"name": "rye bread"}},
            SimilarityResponse
    ),
    (
            'match',
            MatchRequest,
            {"from": "impressions", "where": {"context.user": "veronica"}, "match": "product"},
            MatchResponse
    ),
    ('relate', RelateRequest, {"from": "products", "where": {"$exists": "name"}, "relate": "tags"}, RelateResponse),
    (
            'generic_query',
            GenericQueryRequest,
            {"from": "products", "where": {"name": "Pirkka banana"}, "get": "tags", "orderBy": "$p"},
            HitsResponse
    ),
]
