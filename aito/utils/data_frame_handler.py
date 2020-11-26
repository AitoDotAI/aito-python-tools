"""A utility to read, write, and convert a Pandas DataFrame in accordance to a Aito Table Schema
"""

import logging
from typing import List, Dict, Callable

import pandas as pd

from aito.utils._typing import *
from aito.schema import AitoTableSchema, DataSeriesProperties

LOG = logging.getLogger('DataFrameHandler')


class DataFrameHandler:
    """A handler that supports read, write, and convert a Pandas DataFrame in accordance to a Aito Table Schema
    """
    allowed_format = ['csv', 'json', 'excel', 'ndjson']

    def __init__(self):
        self.default_options = {
            'csv': {'index_col':False, 'error_bad_lines':False, 'warn_bad_lines':True, 'engine':'python'},
            'excel': {},
            'json': {'orient': 'records'},
            'ndjson': {'orient': 'records', 'lines': True}
        }
        self.default_apply_functions = [self._datetime_to_string]

    def _validate_in_out_format(self, in_format: str, out_format: str):
        """Validate the file parameters of the converter
        """
        if in_format not in self.allowed_format:
            raise ValueError(
                f"Expect the input format to be one of {'|'.join(self.allowed_format)} instead of {in_format}"
            )

        if out_format not in self.allowed_format:
            raise ValueError(
                f"Expect the output format to be one of {'|'.join(self.allowed_format)} instead of {out_format}"
            )

    @staticmethod
    def _datetime_to_string(df: pd.DataFrame) -> pd.DataFrame:
        """Convert pandas datetime type to string
        """
        for col in df:
            if df[col].dtypes == 'datetime64[ns]':
                df[col] = df[col].astype(str)
        return df

    @staticmethod
    def _apply_functions_on_df(df: pd.DataFrame, functions: List[Callable]) -> pd.DataFrame:
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

    @staticmethod
    def convert_df_using_aito_table_schema(
            df: pd.DataFrame, table_schema: Union[AitoTableSchema, Dict]
    ) -> pd.DataFrame:
        """convert a pandas DataFrame to match a given Aito table schema

        :param df: input pandas DataFrame
        :type df: pd.DataFrame
        :param table_schema: input table schema
        :type table_schema: an AitoTableSchema object or a Dict, optional
        :raises ValueError: input table schema is invalid
        :raises e: failed to convert
        :return: converted DataFrame
        :rtype: pd.DataFrame
        """
        if not isinstance(table_schema, AitoTableSchema):
            if not isinstance(table_schema, dict):
                raise ValueError("the input table schema must be either an AitoTableSchema object or a dict")
            table_schema = AitoTableSchema.from_deserialized_object(table_schema)

        df_columns = set(df.columns.values)
        table_schema_columns = set(table_schema.columns)

        for col_name in (df_columns - table_schema_columns):
            LOG.warning(f"column `{col_name}` found in the input data but not found in the input schema")
        for col_name in (table_schema_columns - df_columns):
            LOG.warning(f"column `{col_name}` found in the input schema but not found in the input data")

        conversion_map = {}
        for col_name in table_schema_columns.intersection(df_columns):
            col_schema = table_schema[col_name]
            col_df_nullable = df[col_name].isna().any()
            if col_df_nullable and not col_schema.nullable:
                raise ValueError(f"column `{col_name}` is nullable but stated non-nullable in the input schema")
            conversion_map[col_name] = col_schema.to_conversion()

        LOG.debug(f"casting dataframe columns: {conversion_map}")
        converted_df = df
        for col_name in conversion_map:
            conversion = conversion_map[col_name]
            converted_df[col_name] = converted_df[col_name].apply(conversion)

        LOG.debug(f"converted the dataframe according to the schema")
        return converted_df

    def read_file_to_df(self, read_input: FilePathOrBuffer, in_format: str, read_options: Dict = None) -> pd.DataFrame:
        """Read input to a Pandas DataFrame

        :param read_input: read input
        :type read_input: any valid string path, pathlike object, or file-like object (objects with a read() method)
        :param in_format: input format
        :type in_format: str
        :param read_options: dictionary contains arguments for pandas read function, defaults to None
        :type read_options: Dict, optional
        :return: read DataFrame
        :rtype: pd.DataFrame
        """
        LOG.debug(f'reading data from {read_input} to df...')
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
        :type write_output: any valid string path, pathlike object, or file-like object (objects with a read() method)
        :param convert_options: dictionary contains arguments for pandas write function, defaults to None
        :type convert_options: Dict, optional
        """
        convert_functions = {'csv': df.to_csv, 'excel': df.to_excel, 'json': df.to_json, 'ndjson': df.to_json}
        if not convert_options:
            options = self.default_options[out_format]
        else:
            options = convert_options
            options.update(self.default_options[out_format])

        LOG.debug(f'converting to {out_format} and writing to {write_output} with options {options}...')

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
            use_table_schema: Union[AitoTableSchema, Dict] = None
    ) -> pd.DataFrame:
        """Converting input file to expected format, generate or use Aito table schema if specified

        :param read_input: read input
        :type read_input: any valid string path, pathlike object, or file-like object (objects with a read() method)
        :param write_output: write output
        :type write_output: any valid string path, pathlike object, or file-like object (objects with a read() method)
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
        :type use_table_schema: an AitoTableSchema object or a Dict, optional
        :return: converted DataFrame
        :rtype: pd.DataFrame
        """
        self._validate_in_out_format(in_format, out_format)

        df = self.read_file_to_df(read_input, in_format, read_options)

        if apply_functions:
            apply_functions = self.default_apply_functions + apply_functions
        else:
            apply_functions = self.default_apply_functions
        df = self._apply_functions_on_df(df, apply_functions)

        # if no schema is predefined, infer the schema and use the inferred schema
        used_table_schema = use_table_schema
        if used_table_schema is None:
            used_table_schema = AitoTableSchema.infer_from_pandas_data_frame(df)

        df = self.convert_df_using_aito_table_schema(df, used_table_schema)

        if out_format != in_format or convert_options or use_table_schema:
            self.df_to_format(df, out_format, write_output, convert_options)
        return df
