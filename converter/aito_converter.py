import json
import logging
import timeit
from pathlib import Path
from typing import Union, List, Dict, Callable

import pandas as pd


class AitoConverter:
    def __init__(self):
        self.logger = logging.getLogger('AitoConverter')
        self.allowed_format = ['csv', 'json', 'xlsx', 'ndjson']

    def extract_and_validate_file_parameters(self,
                                             out_file_folder: Union[str, Path],
                                             in_file_path: Union[str, Path],
                                             out_format: str,
                                             in_format: str = None,
                                             out_file_name: str = None,
                                             in_file_name: str = None):
        """
        Validate the file parameters of the converter
        :param out_file_folder: path to output file folder
        :param in_file_path: path to input file
        :param out_format: file output format
        :param in_format: if input format is not defined, it will be inferred from input file path suffix
        :param out_file_name: if output file name is not defined, it will be the same as input file name
        :param in_file_name: if input file name is not defined, it will be inferred from input file path stem
        :return: Extracted and validated
                (out_file_folder, in_file_path, out_format, in_format, out_file_name. in_file_name)
        """
        try:
            in_file_path = Path(in_file_path)
            out_file_folder = Path(out_file_folder)
        except Exception as e:
            raise e

        if not in_file_name:
            in_file_name = in_file_path.stem

        if not out_file_name:
            out_file_name = in_file_name

        if not in_format:
            in_format = in_file_path.suffix[1:]

        if in_format not in self.allowed_format:
            raise ValueError(f"Expect input format to be {str(self.allowed_format)} instead of {out_format}")

        if out_format not in self.allowed_format:
            raise ValueError(f"Expect output format to be {str(self.allowed_format)} instead of {out_format}")

        return out_file_folder, in_file_path, out_format, in_format, out_file_name, in_file_name

    @staticmethod
    def generate_aito_table_schema_from_pandas_df(table_df: pd.DataFrame,
                                                  text_analyzer='Whitespace'):
        # TODO: This should not take the whole df but only take a sample
        """
        Return aito schema in dictionary format
        :param table_df: The pandas DataFrame containg table data
        :param text_analyzer: analyzer for text data. It can be also be ['English', 'Finnish', 'Swedish, 'German']
        :return: Aito Table Schema as dict
        """
        type_map = {'string': 'String',
                    'unicode': 'Text',
                    'bytes': 'Text',
                    'floating': 'Decimal',
                    'integer': 'Int',
                    'mixed - integer': 'Decimal',
                    'mixed - integer - float': 'Decimal',
                    'boolean': 'Boolean',
                    'decimal': 'Decimal',
                    'datetime64': 'String',
                    'datetime': 'String',
                    'date': 'String',
                    'timedelta64': 'String',
                    'timedelta': 'String',
                    'time': 'String',
                    'period': 'String',
                    'mixed': 'Text'
                    }
        columns_schema = {}
        for col in table_df.columns.values:
            col_schema = {
                'nullable': True if table_df[col].isna().any().any() else False,
                'type': type_map[pd.api.types.infer_dtype(table_df[col].values, skipna=True)]
            }
            if col_schema['type'] == 'Text':
                col_schema['analyzer'] = text_analyzer
            columns_schema[col] = col_schema

        table_schema = {'type': 'table', 'columns': columns_schema}
        return table_schema

    @staticmethod
    def standard_fix(df: pd.DataFrame):
        # Aito hasn't support datetime data yet. Converting all datetime data to string
        for col in df:
            if df[col].dtypes == 'datetime64[ns]':
                df[col] = df[col].astype(str)
        return df

    def read_file(self, in_file_path: Path, in_format: str, load_options: Dict = None):
        """
        Load a file and return pandas Dataframe
        :param in_file_path: path to input file
        :param in_format: input format
        :param load_options: dictionary contains arguments for pandas read function
        :return:
        """
        start = timeit.default_timer()
        read_functions = {'csv': pd.read_csv, 'xlsx': pd.read_excel, 'json': pd.read_json, 'ndjson': pd.read_json}
        default_options = {'csv': {}, 'xlsx': {}, 'json': {'orient': 'records'},
                           'ndjson': {'orient': 'records', 'lines': True}}

        if not load_options:
            options = default_options[in_format]
        else:
            options = load_options
            options.update(default_options[in_format])
        df = read_functions[in_format](in_file_path, **options)
        self.logger.info(f"Read file {str(in_file_path)} took {timeit.default_timer() - start}")
        return df

    def apply_functions(self, df: pd.DataFrame, functions: List[Callable]):
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
            self.logger.info(f"Applying function {f.__name__} took {timeit.default_timer() - start_f}")
        self.logger.info(f"Applying all functions took {timeit.default_timer() - start}")
        return df

    def to_format(self, df: pd.DataFrame, out_format: str, out_file_name: str, out_file_folder: Path,
                  convert_options: Dict = None):
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
        default_options = {'csv': {}, 'xlsx': {}, 'json': {'orient': 'records'},
                           'ndjson': {'orient': 'records', 'lines': True}}
        options = default_options[out_format] if not convert_options else \
            convert_options.update(default_options[out_format])

        out_file_path = out_file_folder / f"{out_file_name}.{out_format}"
        convert_functions[out_format](out_file_path, **options)
        self.logger.info(f"Convert to {out_format} and write to file took {timeit.default_timer() - start}")

    def convert_file(self,
                     out_file_folder: Union[str, Path],
                     in_file_path: Union[str, Path],
                     out_format: str,
                     in_format: str = None,
                     out_file_name: str = None,
                     in_file_name: str = None,
                     load_options: Dict = None,
                     special_fix_functions: List[Callable] = None,
                     convert_options: Dict = None,
                     generate_aito_schema: bool = True):
        """
        Converting a file into expected format and generate aito schema if required
        :param out_file_folder: path to output file folder
        :param in_file_path: path to input file
        :param out_format: file output format
        :param in_format: if input format is not defined, it will be inferred from input file path suffix
        :param out_file_name: if output file name is not defined, it will be the same as input file name
        :param in_file_name: if input file name is not defined, it will be inferred from input file path stem
        :param load_options: dictionary contains arguments for pandas read function
        :param convert_options: dictionary contains arguments for pandas convert function
        :param special_fix_functions: List of partial functions that will be chained applied to the loaded pd.DataFrame
        :param generate_aito_schema: option to auto generate aito schema
        :return:
        """
        try:
            out_file_folder, in_file_path, out_format, in_format, out_file_name, in_file_name = \
                self.extract_and_validate_file_parameters(out_file_folder, in_file_path, out_format,
                                                          in_format, out_file_name, in_file_name)
        except Exception as e:
            raise e
        df = self.read_file(in_file_path, in_format, load_options)

        if special_fix_functions:
            special_fix_functions = [self.standard_fix] + special_fix_functions
        else:
            special_fix_functions = [self.standard_fix]
        df = self.apply_functions(df, special_fix_functions)

        self.to_format(df, out_format, out_file_name, out_file_folder, convert_options)

        if generate_aito_schema:
            start = timeit.default_timer()
            schema = self.generate_aito_table_schema_from_pandas_df(df)
            self.logger.info(f"Generate aito table schema took took {timeit.default_timer() - start}")
            with (out_file_folder / f"{out_file_name}_schema.json").open(mode='w') as f:
                json.dump(schema, f, indent=4, sort_keys=True)

