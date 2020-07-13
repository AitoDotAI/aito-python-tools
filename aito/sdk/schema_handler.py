import logging
import warnings
from typing import Dict

import pandas as pd

from aito.schema import AitoTableSchema, AitoDataTypeSchema

LOG = logging.getLogger("SchemaHandler")


class SchemaHandler:
    """
    .. deprecated:: 0.3.0

    Use :mod:`~aito.schema` instead

    """
    def __init__(self):
        warnings.warn(
            'SchemaHandler is deprecated and will be removed in a future version. Use the aito_schema module instead',
            category=FutureWarning
        )

    def infer_aito_types_from_pandas_series(self, series: pd.Series, sample_size: int = 100000) -> str:
        """
        .. deprecated:: 0.3.0

        Use :func:`aito.schema.AitoDataTypeSchema.infer_from_samples` instead
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

        Use :func:`aito.schema.AitoTableSchema.infer_from_pandas_dataframe` instead
        """
        warnings.warn(
            'The function SchemaHandler.infer_table_schema_from_pandas_data_frame is deprecated and will be removed '
            'in a future version. Use AitoTableSchema.infer_from_pandas_dataframe instead',
            category=FutureWarning
        )
        return AitoTableSchema.infer_from_pandas_data_frame(df).to_json_serializable()

    def validate_table_schema(self, table_schema: Dict) -> Dict:
        """

        .. deprecated: 0.3.0

        Use :func:`aito.schema.AitoTableSchema.from_deserialized_object` instead
        """
        warnings.warn(
            'The function SchemaHandler.validate_table_schema is deprecated and will be removed '
            'in a future version. Use AitoTableSchema.from_deserialized_object instead',
            category=FutureWarning
        )
        return AitoTableSchema.from_deserialized_object(table_schema).to_json_serializable()
