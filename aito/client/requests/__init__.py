"""Request classes that is sent by an :class:`~aito.client.AitoClient` to an Aito instance

"""

from .data_api_request import UploadEntriesRequest, DeleteEntries, InitiateFileUploadRequest, \
    TriggerFileProcessingRequest, GetFileProcessingRequest
from .job_api_request import CreateJobRequest, GetJobResultRequest, GetJobStatusRequest
from .query_api_request import QueryAPIRequest, SearchRequest, PredictRequest, RecommendRequest, EvaluateRequest, \
    SimilarityRequest, MatchRequest, RelateRequest, GenericQueryRequest
from .schema_api_request import GetDatabaseSchemaRequest, GetTableSchemaRequest, GetColumnSchemaRequest, \
    CreateDatabaseSchemaRequest, CreateTableSchemaRequest, CreateColumnSchemaRequest, DeleteDatabaseSchemaRequest, \
    DeleteTableSchemaRequest, DeleteColumnSchemaRequest
from .aito_request import AitoRequest, BaseRequest, GetVersionRequest