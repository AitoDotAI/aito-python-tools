import logging
from typing import List, Dict

import pandas as pd
from langdetect import detect


class SchemaHandler:
    def __init__(self):
        self.logger = logging.getLogger("SchemaHandler")
        self.pandas_types_to_aito_type = {
            'string': 'Text',
            'unicode': 'Text',
            'bytes': 'Text',
            'floating': 'Decimal',
            'integer': 'Int',
            'mixed - integer': 'Decimal',
            'mixed - integer - float': 'Decimal',
            'decimal': 'Decimal',
            'complex': 'Decimal',
            'categorical': 'String',
            'boolean': 'Boolean',
            'datetime64': 'String',
            'datetime': 'String',
            'date': 'String',
            'timedelta64': 'String',
            'timedelta': 'String',
            'time': 'String',
            'period': 'String',
            'mixed': 'Text'
        }
        self.aito_types_to_pandas_types = {
            'Boolean': 'boolean',
            'Decimal': 'decimal',
            'Int': 'integer',
            'String': 'string',
            'Text': 'string'
        }
        self.sample_size = 100000

    def infer_aito_types_from_values(self, values: List):
        dtypes = pd.api.types.infer_dtype(values, skipna=True)
        return self.pandas_types_to_aito_type[dtypes]

    def generate_table_schema_from_pandas_dataframe(self, table_df: pd.DataFrame):
        """
        Return aito schema in dictionary format
        :param table_df: The pandas DataFrame containing table data
        :return: Aito Table Schema as dict
        """
        self.logger.info("Start inferring schema...")

        rows_count = table_df.shape[0]

        columns_schema = {}
        for col in table_df.columns.values:
            if rows_count >= self.sample_size:
                col_df = table_df[col].sample(self.sample_size)
            else:
                col_df = table_df[col]
            col_data = col_df.values
            col_aito_type = self.infer_aito_types_from_values(col_data)
            col_nullable = col_df.isna().any()

            col_schema = {
                'nullable': True if col_nullable else False,
                'type': col_aito_type
            }
            if col_schema['type'] == 'Text':
                col_text = col_df.str.cat(sep = ' ')
                lang = detect(col_text)
                col_schema['analyzer'] = lang
            columns_schema[col] = col_schema

        table_schema = {'type': 'table', 'columns': columns_schema}
        return table_schema

    def validate_table_schema(self, table_schema: Dict):
        """

        :param table_schema: table schema as dict
        :return:
        """
        def validate_required_arg(object_name: str, object_dict: Dict, arg_name: str):
            if not object_dict[arg_name]:
                raise ValueError(f"{object_name} missing '{arg_name}'")

        def validate_arg_type(object_name: str, object_dict: Dict, arg_name: str, arg_type: object,
                              accepted_values: List = None):
            arg_value = object_dict[arg_name]
            if not isinstance(arg_value, arg_type):
                raise ValueError(f"Invalid {object_name} {arg_name} type")
            if accepted_values:
                if arg_value not in accepted_values:
                    accepted_values_str = accepted_values[0] if len(accepted_values) == 1 \
                        else f"one of {accepted_values}"
                    raise ValueError(f"{object_name} '{arg_name}' must be {accepted_values_str}")

        def validate_arg(object_name: str, object_dict: Dict, arg_name: str, required: bool = False,
                         arg_type: object = None, accepted_values: List = None):
            if required:
                validate_required_arg(object_name, object_dict, arg_name)
            if arg_type:
                validate_arg_type(object_name, object_dict, arg_name, arg_type, accepted_values)

        self.logger.info("Start validating schema...")
        validate_arg('table', table_schema, 'type', True, str, ['table'])
        validate_arg('table', table_schema, 'columns', True, dict)
        for col in table_schema['columns']:
            col_schema = table_schema['columns'][col]
            validate_arg(col, col_schema, 'type', True, str, list(self.aito_types_to_pandas_types.keys()))
            validate_arg(col, col_schema, 'nullable', False, bool)
