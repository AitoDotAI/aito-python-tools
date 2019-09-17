import pandas as pd
from langdetect import detect


class SchemaGeneartor:
    @staticmethod
    def table_schema_from_pandas_dataframe(table_df: pd.DataFrame):
        """
        Return aito schema in dictionary format
        :param table_df: The pandas DataFrame containing table data
        :return: Aito Table Schema as dict
        """
        rows_count = table_df.shape[0]
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
                if rows_count < 1000:
                    sample_text = table_df[col].str.cat(sep=' ')
                else:
                    sample_text = table_df[col].sample(1000).str.cat(sep=' ')
                lang = detect(sample_text)
                col_schema['analyzer'] = lang
            columns_schema[col] = col_schema

        table_schema = {'type': 'table', 'columns': columns_schema}
        return table_schema
