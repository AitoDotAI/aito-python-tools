import logging
from typing import List, Dict

import pandas as pd
from langdetect import detect


class SchemaHandler:
    def __init__(self):
        self.logger = logging.getLogger("SchemaHandler")
        self.pandas_dtypes_name_to_aito_type = {
            'string': 'Text',
            'bytes': 'Text',
            'floating': 'Decimal',
            'integer': 'Int',
            'mixed-integer': 'Decimal',
            'mixed-integer-float': 'Decimal',
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
        self.aito_types_to_python_types = {
            'Boolean': bool,
            'Decimal': float,
            'Int': int,
            'String': str,
            'Text': str
        }

        self.lang_detect_code_to_aito_code = {
            'ko': 'cjk',
            'ja': 'cjk',
            'zh-cn': 'cjk',
            'zh-tw': 'cjk',
            'pt': 'pt-br'
        }

        self.supported_alias_analyzer = ['standard', 'whitespace', 'ar', 'hy', 'eu', 'pt-br', 'bg', 'ca', 'cjk', 'cs',
                                         'da', 'nl', 'en', 'fi', 'fr', 'gl', 'de', 'el', 'hi', 'hu', 'id', 'ga', 'it',
                                         'lv', 'no', 'fa', 'pt', 'ro', 'ru', 'es', 'sv', 'th', 'tr']
        self.sample_size = 100000

    def infer_aito_types_from_pandas_series(self, series: pd.Series, sample_size = 100000) -> str:
        sampled_values = series.values if len(series) < sample_size else series.sample(sample_size).values
        inferred_dtype = pd.api.types.infer_dtype(sampled_values)
        if inferred_dtype not in self.pandas_dtypes_name_to_aito_type:
            raise Exception(f"Cannot not infer aito type from dtype {inferred_dtype}")
        return self.pandas_dtypes_name_to_aito_type[inferred_dtype]

    def infer_table_schema_from_pandas_data_frame(self, table_df: pd.DataFrame) -> Dict:
        """
        Return aito schema in dictionary format
        :param table_df: The pandas DataFrame containing table data
        :return: Aito Table Schema as dict
        """
        self.logger.info("Start inferring schema...")

        rows_count = table_df.shape[0]

        columns_schema = {}
        for col in table_df.columns.values:
            col_df = table_df[col]
            try:
                col_aito_type = self.infer_aito_types_from_pandas_series(table_df[col], self.sample_size)
            except Exception as e:
                raise Exception(f"Cannot infer aito type of column {col}: {e}")
            col_na_count = col_df.isna().sum()
            col_schema = {
                'nullable': True if col_na_count > 0 else False,
                'type': col_aito_type
            }
            if col_schema['type'] == 'Text':
                col_unique_val_count = col_df.nunique()
                col_non_na_count = rows_count - col_na_count
                if (col_non_na_count / col_unique_val_count) > 2:
                    col_schema['type'] = 'String'
                else:
                    col_text = col_df.str.cat(sep=' ')
                    try:
                        detected_lang = detect(col_text)
                        if detected_lang in self.lang_detect_code_to_aito_code:
                            detected_lang = self.lang_detect_code_to_aito_code[detected_lang]
                    except:
                        detected_lang = None
                    if detected_lang and detected_lang in self.supported_alias_analyzer:
                        col_schema['analyzer'] = detected_lang

            columns_schema[col] = col_schema

        table_schema = {'type': 'table', 'columns': columns_schema}
        self.logger.info("Finished inferring schema")
        return table_schema

    def validate_table_schema(self, table_schema: Dict):
        """

        :param table_schema: table schema as dict
        :return:
        """
        def validate_required_arg(object_name: str, object_dict: Dict, arg_name: str):
            if arg_name not in object_dict:
                raise ValueError(f"{object_name} missing '{arg_name}'")

        def validate_arg_type(object_name: str, object_dict: Dict, arg_name: str, arg_type: type,
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
                         arg_type: type = None, accepted_values: List = None):
            if required:
                validate_required_arg(object_name, object_dict, arg_name)
            if arg_name in object_dict and arg_type:
                validate_arg_type(object_name, object_dict, arg_name, arg_type, accepted_values)

        self.logger.info("Start validating schema...")
        validate_arg('table', table_schema, 'type', True, str, ['table'])
        validate_arg('table', table_schema, 'columns', True, dict)
        for col in table_schema['columns']:
            col_schema = table_schema['columns'][col]
            validate_arg(col, col_schema, 'type', True, str, list(self.aito_types_to_python_types.keys()))
            validate_arg(col, col_schema, 'nullable', False, bool)
        self.logger.info("Finished validating schema")
        return table_schema
