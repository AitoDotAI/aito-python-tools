"""Generic Aito Response Class"""

import logging
from typing import Dict, Any

from aito.utils._json_format import JsonFormat

LOG = logging.getLogger('AitoResponse')


class BaseResponse(JsonFormat):
    """The base class for the AitoClient request response

    """
    def __init__(self, json: Dict):
        """
        :param json: the original JSON response of the request
        :type json: Dict
        """
        self.json_schema_validate(json)
        self._json = json

    @property
    def json(self):
        """the original JSON response of the request

        :rtype: Dict
        """
        return self._json

    def __getitem__(self, item):
        if item not in self._json:
            raise KeyError(f'Response does not contain field `{item}`')
        return self._json[item]

    def __contains__(self, item):
        return item in self._json

    def __iter__(self):
        return iter(self._json)

    def __len__(self):
        return len(self._json)

    @classmethod
    def json_schema(cls):
        return {'type': 'object'}

    def to_json_serializable(self):
        return self._json

    @classmethod
    def from_deserialized_object(cls, obj: Any):
        return cls(obj)


class GetVersionResponse(BaseResponse):
    """Response of the get version request"""
    @property
    def version(self) -> str:
        """the Aito instance version

        :rtype: str
        """
        return self.__getitem__('version')
