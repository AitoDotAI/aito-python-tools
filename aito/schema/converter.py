import json
import logging
import timeit
from pathlib import Path
from typing import Union, List, Dict, Callable
from config import set_up_logger
from aito.schema.schema_generator import SchemaGeneartor

import pandas as pd


class Converter:
    allowed_format = ['csv', 'json', 'xlsx', 'ndjson']

    def __init__(self):
        set_up_logger('converter')
        self.logger = logging.getLogger('AitoConverter')
        self.default_options = {
            'csv': {},
            'xlsx': {},
            'json': {'orient': 'records'},
            'ndjson': {'orient': 'records', 'lines': True}
        }
        self.default_apply_functions = [self.datetime_to_string]

    def validate_parameters(self, in_format: str, out_format: str):
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

    def read_file_to_df(self, read_input, in_format: str, load_options: Dict = None) -> pd.DataFrame:
        """
        Load a file and return pandas Dataframe
        :param read_input: path to or buffer of input
        :param in_format: input format
        :param load_options: dictionary contains arguments for pandas read function
        :return:
        """
        start = timeit.default_timer()
        read_functions = {'csv': pd.read_csv, 'xlsx': pd.read_excel, 'json': pd.read_json, 'ndjson': pd.read_json}

        if not load_options:
            options = self.default_options[in_format]
        else:
            options = load_options
            options.update(self.default_options[in_format])
        df = read_functions[in_format](read_input, **options)
        self.logger.info(f"Read input took {(timeit.default_timer() - start):.3f}")
        return df

    def apply_functions_on_df(self, df: pd.DataFrame, functions: List[Callable]) -> pd.DataFrame:
        """
        Applying functions sequentially to a dataframe
        :param df:
        :param functions:
        :return:
        """
        start = timeit.default_timer()
        for f in functions:
            start_f = timeit.default_timer()
            df = f(df)
            self.logger.info(f"Applying function {f.__name__} took {(timeit.default_timer() - start_f):.3f}")
        self.logger.info(f"Applying all functions took {(timeit.default_timer() - start):.3f}")
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
        start = timeit.default_timer()
        convert_functions = {'csv': df.to_csv, 'xlsx': df.to_excel, 'json': df.to_json, 'ndjson': df.to_json}
        if not convert_options:
            options = self.default_options[out_format]
        else:
            options = convert_options
            options.update(self.default_options[out_format])

        convert_functions[out_format](write_output, **options)
        self.logger.info(f"Convert to {out_format} and write to file took {(timeit.default_timer() - start):.3f}")

    def convert_file(self,
                     read_input,
                     write_output,
                     in_format: str,
                     out_format: str,
                     read_options: Dict = None,
                     convert_options: Dict = None,
                     apply_functions: List[Callable[..., pd.DataFrame]] = None,
                     generate_aito_schema: Path = None):
        """
        Converting a file into expected format and generate aito schema if required
        :param read_input: filepath to input or input buffer
        :param write_output: filepath to output or output buffer
        :param in_format: if input format is not defined, it will be inferred from input file path suffix
        :param out_format: file output format
        :param read_options: dictionary contains arguments for pandas read function
        :param convert_options: dictionary contains arguments for pandas convert function
        :param apply_functions: List of partial functions that will be chained applied to the loaded pd.DataFrame
        :param generate_aito_schema: option to auto generate aito schema
        :return:
        """
        try:
            self.validate_parameters(in_format, out_format)
        except Exception as e:
            raise e
        df = self.read_file_to_df(read_input, in_format, read_options)

        if apply_functions:
            apply_functions = self.default_apply_functions + apply_functions
        else:
            apply_functions = self.default_apply_functions
        df = self.apply_functions_on_df(df, apply_functions)

        self.df_to_format(df, out_format, write_output, convert_options)

        if generate_aito_schema:
            generator = SchemaGeneartor()
            schema = generator.table_schema_from_pandas_dataframe(df)
            with generate_aito_schema.open(mode='w') as f:
                json.dump(schema, f, indent=4, sort_keys=True)

