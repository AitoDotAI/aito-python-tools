import logging
import logging
import warnings
from typing import Dict

import pandas as pd

from aito.sdk.aito_schema import AitoTableSchema, AitoDataTypeSchema

LOG = logging.getLogger("SchemaHandler")


class SchemaHandler:
    def __init__(self):
        self.aito_types_to_python_types = {
            'Boolean': bool,
            'Decimal': float,
            'Int': int,
            'String': str,
            'Text': str
        }

    def infer_aito_types_from_pandas_series(self, series: pd.Series, sample_size: int = 100000) -> str:
        """
        .. deprecated:: 0.3.0

        Use :func:`~aito.sdk.aito_schema.AitoDataTypeSchema.infer_from_samples` instead
        """
        warnings.warn(
            'The function SchemaHandler.infer_aito_types_from_pandas_series is deprecated and will be removed '
            'in a future version. Use AitoDataTypeSchema.infer_from_samples instead',
            category=FutureWarning
        )
        return AitoDataTypeSchema._infer_from_pandas_series(series, sample_size).aito_dtype

    def infer_table_schema_from_pandas_data_frame(self, df: pd.DataFrame) -> Dict:
        """

        .. deprecated: 0.3.0

        Use :func:`~aito.sdk.aito_schema.AitoTableSchema.infer_from_pandas_dataframe` instead
        """
        warnings.warn(
            'The function SchemaHandler.infer_table_schema_from_pandas_data_frame is deprecated and will be removed '
            'in a future version. Use AitoTableSchema.infer_from_pandas_dataframe instead',
            category=FutureWarning
        )
        return AitoTableSchema.infer_from_pandas_dataframe(df).to_json_serializable()

    def validate_table_schema(self, table_schema: Dict) -> Dict:
        """Validate Aito Schema

        :param table_schema: input table schema
        :type table_schema: Dict
        :raises ValueError: table schema is invalid
        :return: table schema if valid
        :rtype: Dict
        """
        warnings.warn(
            'The function SchemaHandler.validate_table_schema is deprecated and will be removed '
            'in a future version. Use AitoTableSchema.from_deserialized_object instead',
            category=FutureWarning
        )
        return AitoTableSchema.from_deserialized_object(table_schema).to_json_serializable()
