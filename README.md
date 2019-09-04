# aito-python-tools

A python library that contains support tools when using Aito.
The library use [pipenv](https://pipenv-fork.readthedocs.io/en/latest/) for package management. 

## converter
* Acceptable formats: [csv, xlsx, json, ndjson] 
* Convert from acceptable format to acceptable format
* Generate a naive Aito Schema:
    * Use "whitespace" as the default analyzer for text type
    * All date time format will be coverted to  string type
* Some special cases:
    1. UnicodeDecodeError when loading file: Add ```engine: python``` to load_options
* How to use: 
    * Installation: ```pipenv install```
    * Functions:
        * AitoConverter.read_file: read a file to pandas DataFrame.
        * AitoConverter.to_format: convert a pandas data frame to a acceptable format.
        * AitoConverter.generate_aito_table_schema_from_pandas_df: generate an Aito table schema from a pandas DataFrame.
        * AitoConverter.convert_file: convert a file into a acceptable format and possible generate Aito table schema.