"""Aito `Schema API <https://aito.ai/docs/api/#database-api>`__ Response Class"""

from abc import ABC, abstractmethod
from typing import Type

from .aito_response import BaseResponse
from aito.schema import AitoSchema, AitoDatabaseSchema, AitoTableSchema, AitoColumnTypeSchema


class _SchemaResponse(BaseResponse, ABC):
    """Response that contains a schema component"""
    @property
    @abstractmethod
    def schema_cls(self) -> Type[AitoSchema]:
        """the class of the schema component

        :rtype: Type[AitoSchema]
        """
        pass

    @property
    def schema(self) -> AitoSchema:
        """return an instance of the appropriate AitoSchema

        :rtype: AitoSchema
        """
        return self.schema_cls.from_deserialized_object(self._json)


class DatabaseSchemaResponse(_SchemaResponse):
    """Response that contains a database schema"""
    @property
    def schema_cls(self) -> Type[AitoSchema]:
        return AitoDatabaseSchema


class TableSchemaResponse(_SchemaResponse):
    """Response that contains a table schema"""
    @property
    def schema_cls(self) -> Type[AitoSchema]:
        return AitoTableSchema


class ColumnSchemaResponse(_SchemaResponse):
    """Response that contains a column schema"""
    @property
    def schema_cls(self) -> Type[AitoSchema]:
        return AitoColumnTypeSchema