import json
import logging
from pathlib import Path
from typing import List, Dict, Callable
import io
import pandas as pd

from aito.schema.schema_handler import SchemaHandler


class DataFrameHandler:
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
        """
        Validate the file parameters of the converter
        :param in_format: input format
        :param out_format: output format
        """
        if in_format not in self.allowed_format:
            raise ValueError(f"Expect input format to be {str(self.allowed_format)} instead of {in_format}")

        if out_format not in self.allowed_format:
            raise ValueError(f"Expect output format to be {str(self.allowed_format)} instead of {out_format}")

    @staticmethod
    def datetime_to_string(df: pd.DataFrame) -> pd.DataFrame:
        # Aito hasn't support datetime data yet. Converting all datetime data to string
        for col in df:
            if df[col].dtypes == 'datetime64[ns]':
                df[col] = df[col].astype(str)
        return df

    @staticmethod
    def apply_functions_on_df(df: pd.DataFrame, functions: List[Callable]) -> pd.DataFrame:
        """
        Applying functions sequentially to a dataframe
        :param df:
        :param functions:
        :return:
        """
        for f in functions:
            df = f(df)
        return df

    def convert_df_from_aito_table_schema(self, df: pd.DataFrame, table_schema: Dict):
        self.schema_handler.validate_table_schema(table_schema)

        columns_schema = table_schema['columns']
        df_columns = set(df.columns.values)
        table_schema_columns = set(columns_schema.keys())

        for col in (df_columns - table_schema_columns):
            self.logger.warn(f"Column '{col}' found in the input data but not found in the input schema")
        for col in (table_schema_columns - df_columns):
            self.logger.warn(f"Column '{col}' found in the input schema but not found in the input data")

        for col in table_schema_columns.intersection(df_columns):
            col_schema = columns_schema[col]
            col_schema_nullable = True if ('nullable' not in col_schema or col_schema['nullable']) else False
            if not col_schema_nullable and df[col].isna().any():
                raise ValueError(f"Column '{col}' is nullable but stated non-nullable in the input schema")
            col_schema_dtypes = self.schema_handler.aito_types_to_pandas_dtypes[col_schema['type']]
            if df[col].dtype.name not in col_schema_dtypes:
                self.logger.info(f"Converting column '{col}' to {col_schema['type']} type according to the schema...")
                try:
                    df[col] = df[col].astype(col_schema_dtypes[0], skipna=True)
                except Exception as e:
                    self.logger.error(f'Conversion error: {e}')
                    raise e
        return df

    def read_file_to_df(self, read_input, in_format: str, read_options: Dict = None) -> pd.DataFrame:
        """
        Load a file and return pandas Dataframe
        :param read_input: path to or buffer of input
        :param in_format: input format
        :param read_options: dictionary contains arguments for pandas read function
        :return:
        """
        if isinstance(read_input, io.TextIOWrapper):
            self.logger.info("Start reading from standard input...")
        else:
            self.logger.info(f"Start reading input from {read_input}...")
        read_functions = {'csv': pd.read_csv, 'excel': pd.read_excel, 'json': pd.read_json, 'ndjson': pd.read_json}

        if not read_options:
            options = self.default_options[in_format]
        else:
            options = read_options
            options.update(self.default_options[in_format])
        df = read_functions[in_format](read_input, **options)
        return df

    def df_to_format(self, df: pd.DataFrame, out_format: str, write_output, convert_options: Dict = None):
        """

        :param df:
        :param out_format:
        :param write_output:
        :param convert_options: dictionary contains arguments for pandas convert function
        :return:
        """
        """

        :param df:
        :param out_format: file output format
        :param out_file_name: if output file name is not defined, it will be the same as input file name
        :param out_file_folder: path to output file folder
        :param convert_options: dictionary contains arguments for pandas convert function
        :return:
        """
        self.logger.info(f"Start converting to {out_format} and writing to output...")
        convert_functions = {'csv': df.to_csv, 'excel': df.to_excel, 'json': df.to_json, 'ndjson': df.to_json}
        if not convert_options:
            options = self.default_options[out_format]
        else:
            options = convert_options
            options.update(self.default_options[out_format])

        convert_functions[out_format](write_output, **options)

    def convert_file(self,
                     read_input,
                     write_output,
                     in_format: str,
                     out_format: str,
                     read_options: Dict = None,
                     convert_options: Dict = None,
                     apply_functions: List[Callable[..., pd.DataFrame]] = None,
                     create_table_schema: Path = None,
                     use_table_schema: Dict = None):
        """
        Converting a file into expected format and generate aito schema if required
        :param read_input: filepath to input or input buffer
        :param write_output: filepath to output or output buffer
        :param in_format: input format
        :param out_format: output format
        :param read_options: dictionary contains arguments for pandas read function
        :param convert_options: dictionary contains arguments for pandas convert function
        :param apply_functions: List of partial functions that will be chained applied to the loaded pd.DataFrame
        :param create_table_schema: option to auto generate aito schema
        :param use_table_schema: use an aito schema to dictates data types and convert the data
        :return:
        """
        try:
            self.validate_in_out_format(in_format, out_format)
        except Exception as e:
            raise e

        df = self.read_file_to_df(read_input, in_format, read_options)

        if apply_functions:
            apply_functions = self.default_apply_functions + apply_functions
        else:
            apply_functions = self.default_apply_functions
        df = self.apply_functions_on_df(df, apply_functions)

        if use_table_schema:
            df = self.convert_df_from_aito_table_schema(df, use_table_schema)

        if out_format != in_format or convert_options:
            self.df_to_format(df, out_format, write_output, convert_options)
        else:
            self.logger.info("Output format is the same as input format. No conversion is done")

        if create_table_schema:
            schema = self.schema_handler.generate_table_schema_from_pandas_dataframe(df)
            with create_table_schema.open(mode='w') as f:
                json.dump(schema, f, indent=4, sort_keys=True)

