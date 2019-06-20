import json
import logging
import timeit
from pathlib import Path
from typing import Union, List, Dict

import pandas as pd

from config import set_up_logger


class AitoConverter:
    def __init__(self):
        set_up_logger('converter')
        self.logger = logging.getLogger('AitoConverter')
        self.allowed_format = ['csv', 'json', 'xlsx', 'ndjson']

    def extract_and_validate_file_parameters(self,
                                             output_file_folder: Union[str, Path],
                                             input_file_path: Union[str, Path],
                                             output_format: str,
                                             input_format: str = None,
                                             output_file_name: str = None,
                                             input_file_name: str = None):
        """
        Validate the file parameters of the converter
        :param output_file_folder: path to output file folder
        :param input_file_path: path to input file
        :param output_format: file output format
        :param input_format: if input format is not defined, it will be inferred from input file path suffix
        :param output_file_name: if output file name is not defined, it will be the same as input file name
        :param input_file_name: if input file name is not defined, it will be inferred from input file path stem
        :return: Extracted and validated
                (output_file_folder, input_file_path, output_format, input_format, output_file_name. input_file_name)
        """
        try:
            input_file_path = Path(input_file_path)
            output_file_folder = Path(output_file_folder)
        except Exception as e:
            raise e

        if not input_file_name:
            input_file_name = input_file_path.stem

        if not output_file_name:
            output_file_name = input_file_name

        if not input_format:
            input_format = input_file_path.suffix.replace('.', '')

        if input_format not in self.allowed_format:
            raise ValueError(f"Expect input format to be {str(self.allowed_format)} instead of {output_format}")

        if output_format not in self.allowed_format:
            raise ValueError(f"Expect output format to be {str(self.allowed_format)} instead of {output_format}")

        return output_file_folder, input_file_path, output_format, input_format, output_file_name, input_file_name

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
        type_map = {'string': 'Text',
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

    def convert_file(self,
                     output_file_folder: Union[str, Path],
                     input_file_path: Union[str, Path],
                     output_format: str,
                     input_format: str = None,
                     output_file_name: str = None,
                     input_file_name: str = None,
                     load_options: Dict = None,
                     special_fix_functions: List = None,
                     convert_options: Dict = None,
                     generate_aito_schema: bool = True):
        """
        Converting a file into expected format and generate aito schema if required
        :param output_file_folder: path to output file folder
        :param input_file_path: path to input file
        :param output_format: file output format
        :param input_format: if input format is not defined, it will be inferred from input file path suffix
        :param output_file_name: if output file name is not defined, it will be the same as input file name
        :param input_file_name: if input file name is not defined, it will be inferred from input file path stem
        :param load_options: dictionary contains arguments for pandas read function
        :param convert_options: dictionary contains arguments for pandas convert function
        :param special_fix_functions: List of partial functions that will be chained applied to the loaded pd.DataFrame.
        :param generate_aito_schema: option to auto generate aito schema
        :return:
        """
        start = timeit.default_timer()
        try:
            output_file_folder, input_file_path, output_format, input_format, output_file_name, input_file_name = \
                self.extract_and_validate_file_parameters(output_file_folder, input_file_path, output_format,
                                                          input_format, output_file_name, input_file_name)
        except Exception as e:
            raise e

        read_functions = {'csv': pd.read_csv, 'xlsx': pd.read_excel, 'json': pd.read_json, 'ndjson': pd.read_json}
        default_options = {'csv': {}, 'xlsx': {}, 'json': {'orient': 'records'},
                           'ndjson': {'orient': 'records', 'lines': True}}

        if not load_options:
            load_options = default_options[input_format]
        else:
            load_options.update(default_options[input_format])

        data = read_functions[input_format](input_file_path, **load_options)
        self.logger.info(f"Load file {str(input_file_path)} took {timeit.default_timer() - start}")

        start = timeit.default_timer()

        if special_fix_functions:
            start = timeit.default_timer()
            for f in special_fix_functions:
                data = f(data)
            self.logger.info(f"Applying functions to dataframe took {timeit.default_timer() - start}")

        if not convert_options:
            convert_options = default_options[output_format]
        else:
            convert_options.update(default_options[output_format])

        convert_functions = {'csv': data.to_csv, 'xlsx': data.to_excel, 'json': data.to_json, 'ndjson': data.to_json}
        output_file_path = output_file_folder / f"{output_file_name}.{output_format}"
        convert_functions[output_format](output_file_path, **convert_options)
        self.logger.info(f"Convert to {output_format} and write to file took {timeit.default_timer() - start}")

        if generate_aito_schema:
            start = timeit.default_timer()
            schema = self.generate_aito_table_schema_from_pandas_df(data)
            print(schema)
            self.logger.info(f"Generate aito table schema took took {timeit.default_timer() - start}")
            with (output_file_folder / f"{output_file_name}_schema.json").open(mode='w') as f:
                json.dump(schema, f, indent=4, sort_keys=True)