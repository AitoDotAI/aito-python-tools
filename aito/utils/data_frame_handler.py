import io
import logging
from typing import List, Dict, Callable

import numpy as np
import pandas as pd

from aito.utils._typing import *
from aito.utils.schema_handler import SchemaHandler


class DataFrameHandler:
    """Pandas DataFrame handler
    """
    allowed_format = ['csv', 'json', 'excel', 'ndjson']

    def __init__(self):
        self.logger = logging.getLogger('DataFrameHandler')
        self.default_options = {
            'csv': {},
            'excel': {},
            'json': {'orient': 'records'},
            'ndjson': {'orient': 'records', 'lines': True}
        }
        self.default_apply_functions = [self.datetime_to_string]
        self.schema_handler = SchemaHandler()

    def validate_in_out_format(self, in_format: str, out_format: str):
        """Validate the file parameters of the converter

        :param in_format: input format
        :type in_format: str
        :param out_format: output format
        :type out_format: str
        :raises ValueError: Unexpected input format or output format
        """
        if in_format not in self.allowed_format:
            raise ValueError(f"Expect input format to be {str(self.allowed_format)} instead of {in_format}")

        if out_format not in self.allowed_format:
            raise ValueError(f"Expect output format to be {str(self.allowed_format)} instead of {out_format}")

    @staticmethod
    def datetime_to_string(df: pd.DataFrame) -> pd.DataFrame:
        """Convert pandas datetime type to string

        :param df: input pandas DataFrame
        :type df: pd.DataFrame
        :return: converted pandas DataFrame
        :rtype: pd.DataFrame
        """
        for col in df:
            if df[col].dtypes == 'datetime64[ns]':
                df[col] = df[col].astype(str)
        return df

    @staticmethod
    def apply_functions_on_df(df: pd.DataFrame, functions: List[Callable]) -> pd.DataFrame:
        """Applying partial functions to a dataframe

        :param df: input pandas DataFrame
        :type df: pd.DataFrame
        :param functions: list of partial functions that will be applied to the loaded pd.DataFrame
        :type functions: List[Callable]
        :return: output DataFrame
        :rtype: pd.DataFrame
        """
        for f in functions:
            df = f(df)
        return df

    def convert_df_using_aito_table_schema(self, df: pd.DataFrame, table_schema: Dict) -> pd.DataFrame:
        """Convert a pandas DataFrame to match a given Aito table schema

        :param df: input pandas DataFrame
        :type df: pd.DataFrame
        :param table_schema: input table schema
        :type table_schema: Dict
        :raises ValueError: input table schema is invalid
        :raises e: failed to convert
        :return: converted DataFrame
        :rtype: pd.DataFrame
        """
        self.schema_handler.validate_table_schema(table_schema)

        columns_schema = table_schema['columns']
        df_columns = set(df.columns.values)
        table_schema_columns = set(columns_schema.keys())

        for col in (df_columns - table_schema_columns):
            self.logger.warning(f"Column '{col}' found in the input data but not found in the input schema")
        for col in (table_schema_columns - df_columns):
            self.logger.warning(f"Column '{col}' found in the input schema but not found in the input data")

        for col in table_schema_columns.intersection(df_columns):
            col_schema = columns_schema[col]
            col_schema_nullable = True if ('nullable' not in col_schema or col_schema['nullable']) else False
            if not col_schema_nullable and df[col].isna().any():
                raise ValueError(f"Column '{col}' is nullable but stated non-nullable in the input schema")
            col_aito_type = col_schema['type']
            col_inferred_aito_type = self.schema_handler.infer_aito_types_from_pandas_series(df[col])
            self.logger.debug(f"Column '{col}' inferred aito type is {col_inferred_aito_type}")
            if col_inferred_aito_type != col_aito_type and \
                    not {col_aito_type, col_inferred_aito_type}.issubset({'Text', 'String'}):
                col_python_type = self.schema_handler.aito_types_to_python_types[col_aito_type]
                self.logger.info(f"Converting column '{col}' to {col_aito_type} type according to the schema")
                self.logger.debug(f"Casting column '{col}' to {col_python_type}")
                try:
                    df[col] = df[col].apply(lambda cell: col_python_type(cell) if not np.isnan(cell) else np.nan)
                except Exception as e:
                    self.logger.error(f'Conversion error: {e}')
                    raise e
        return df

    def read_file_to_df(self, read_input: FilePathOrBuffer, in_format: str, read_options: Dict = None) -> pd.DataFrame:
        """Read input to a Pandas DataFrame

        :param read_input: read input
        :type read_input: FilePathOrBuffer
        :param in_format: input format
        :type in_format: str
        :param read_options: dictionary contains arguments for pandas read function, defaults to None
        :type read_options: Dict, optional
        :return: read DataFrame
        :rtype: pd.DataFrame
        """
        self.logger.debug(f'Star reading from {read_input}...')
        read_functions = {'csv': pd.read_csv, 'excel': pd.read_excel, 'json': pd.read_json, 'ndjson': pd.read_json}

        if not read_options:
            options = self.default_options[in_format]
        else:
            options = read_options
            options.update(self.default_options[in_format])
        df = read_functions[in_format](read_input, **options)
        return df

    def df_to_format(
            self,
            df: pd.DataFrame,
            out_format: str,
            write_output: FilePathOrBuffer,
            convert_options: Dict = None
        ):
        """Write a Pandas DataFrame

        :param df: input DataFrame
        :type df: pd.DataFrame
        :param out_format: output format
        :type out_format: str
        :param write_output: write output
        :type write_output: FilePathOrBuffer
        :param convert_options: dictionary contains arguments for pandas write function, defaults to None
        :type convert_options: Dict, optional
        """
        self.logger.info(f"Start converting to {out_format} and writing to output...")
        convert_functions = {'csv': df.to_csv, 'excel': df.to_excel, 'json': df.to_json, 'ndjson': df.to_json}
        if not convert_options:
            options = self.default_options[out_format]
        else:
            options = convert_options
            options.update(self.default_options[out_format])

        convert_functions[out_format](write_output, **options)

    def convert_file(
            self,
            read_input: FilePathOrBuffer,
            write_output: FilePathOrBuffer,
            in_format: str,
            out_format: str,
            read_options: Dict = None,
            convert_options: Dict = None,
            apply_functions: List[Callable[..., pd.DataFrame]] = None,
            use_table_schema: Dict = None
        ) -> pd.DataFrame:
        """Converting input file to expected format, generate or use Aito table schema if specified

        :param read_input: read input
        :type read_input: FilePathOrBuffer
        :param write_output: write output
        :type write_output: FilePathOrBuffer
        :param in_format: input format
        :type in_format: str
        :param out_format: output format
        :type out_format: str
        :param read_options: dictionary contains arguments for pandas read function, defaults to None
        :type read_options: Dict, optional
        :param convert_options: dictionary contains arguments for pandas write function, defaults to None
        :type convert_options: Dict, optional
        :param apply_functions: list of partial functions that will be applied to the loaded pd.DataFrame, defaults to None
        :type apply_functions: List[Callable[..., pd.DataFrame]], optional
        :param use_table_schema: use an aito schema to dictates data types and convert the data, defaults to None
        :type use_table_schema: Dict, optional
        :return: converted DataFrame
        :rtype: pd.DataFrame
        """
        self.validate_in_out_format(in_format, out_format)

        df = self.read_file_to_df(read_input, in_format, read_options)

        if apply_functions:
            apply_functions = self.default_apply_functions + apply_functions
        else:
            apply_functions = self.default_apply_functions
        df = self.apply_functions_on_df(df, apply_functions)

        if use_table_schema:
            df = self.convert_df_using_aito_table_schema(df, use_table_schema)

        if out_format != in_format or convert_options or use_table_schema:
            self.df_to_format(df, out_format, write_output, convert_options)
        return df
