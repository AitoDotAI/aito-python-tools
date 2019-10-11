# aito-python-tools 
[![PyPI](https://img.shields.io/pypi/pyversions/aitoai?style=plastic)](https://github.com/AitoDotAI/aito-python-tools) [![PyPI version](https://badge.fury.io/py/aitoai.svg)](https://badge.fury.io/py/aitoai)

A useful library for [Aito](https://aito.ai/) users containg: 
* CLI for using Aito
* Integration with [Pandas](https://pandas.pydata.org/)


## Installation

To install with pip, run: `pip install aitoai` 

To install from source, first clone the repository and then run: `python setup.py install`

## Basic Usage

### Command line interface support tools

```bash
➜ usage:  aito [-h] <action> [<args>]
        To see help text, you can run:
            aito -h
            aito <action> -h

        The most commonly actions are:
            infer-table-schema  infer Aito table schema from a file
            convert             convert data of table entries into ndjson (for file-upload) or json (for batch-upload)
            client              set up a client and perform CRUD operations
        

positional arguments:
  action      action to perform

optional arguments:
  -h, --help  show this help message and exit
```

***NOTE:*** For client action, remember to set up your Aito instance, either through environment variable or dotenv 
file or using the command line arguments

For addition guide of the cli tool, see [Using the Aito CLI page](docs/cli.md)

### Integrating with [pandas](https://pandas.pydata.org/)

* Generate Aito Schema from a pandas DataFrame:
  ```python
  from aito.schema_handler import SchemaHandler

  schema_handler = SchemaHandler()
  schema_handler.generate_table_schema_from_pandas_dataframe(df)
  ```

## Feedback & bug reports
We take our quality seriously and aim for the smoothest developer experience possible. If you run into problems, please send an email to support@aito.ai containing reproduction steps and we'll fix it as soon as possible.

## License
[MIT License](LICENSE)

## [Change logs](docs/change_logs.md)