"""Response classes returned by the :class:`~aito.client.aito_client.AitoClient`"""
from .aito_response import BaseResponse, GetVersionResponse
from .job_api_response import CreateJobResponse, GetJobStatusResponse
from .query_api_response import BaseHit, ScoredHit, ProbabilityHit, RelateHit, HitsResponse, SearchResponse, \
    PredictResponse, RecommendResponse, SimilarityResponse, MatchResponse, RelateResponse, EvaluateResponse
from .schema_api_response import DatabaseSchemaResponse, TableSchemaResponse, ColumnSchemaResponse
