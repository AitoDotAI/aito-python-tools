"""Aito `Job API <https://aito.ai/docs/api/#post-api-v1-jobs-query>`__ Response Class"""

from typing import Optional

from .aito_response import BaseResponse


class CreateJobResponse(BaseResponse):
    """Response of the `Create job <https://aito.ai/docs/api/#post-api-v1-jobs-query>`__
    containing the job information
    """
    @property
    def id(self) -> str:
        """the id of a job"""
        return self.__getitem__('id')

    @property
    def started_at(self) -> str:
        """when the job was started"""
        return self.__getitem__('startedAt')


class GetJobStatusResponse(BaseResponse):
    """Response of the `Get job status <https://aito.ai/docs/api/#get-api-v1-jobs-uuid>`__"""

    @property
    def id(self) -> str:
        """the id of the job session"""
        return self.__getitem__('id')

    @property
    def started_at(self) -> str:
        """when the job was started"""
        return self.__getitem__('startedAt')

    @property
    def finished(self) -> bool:
        """if the job has finished"""
        return self.__contains__('finishedAt')

    @property
    def expires_at(self) -> Optional[str]:
        """when the job result will not be available, returns None if job hasn't finished"""
        return self.__getitem__('startedAt') if self.finished else None