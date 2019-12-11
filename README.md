# aito-python-tools
[![PyPI](https://img.shields.io/pypi/pyversions/aitoai?style=plastic)](https://github.com/AitoDotAI/aito-python-tools) [![PyPI version](https://badge.fury.io/py/aitoai.svg)](https://badge.fury.io/py/aitoai)

A useful library for [Aito](https://aito.ai/) users containg:
* CLI for using Aito
* Integration with [Pandas](https://pandas.pydata.org/)


## Installation

To install with pip, run: `pip install aitoai`

To install from source, first clone the repository and then run: `python setup.py install`

***Additional features***:
* The Aito CLI supports integration with your SQL database.
To enable this feature, please follow the instructions [here](docs/sql_functions.md)

* The Aito CLI supports tab completion using [argcomplete](https://argcomplete.readthedocs.io/en/latest/).
More instructions can be found [here](docs/cli.md/#tab-completion)

## Basic Usage

### Aito Command line interface

```bash
aito -h
usage: aito [-h] <action> ...

optional arguments:
  -h, --help          show this help message and exit

action:
  action to perform

  <action>
    infer-table-schema
                      infer an Aito table schema from a file
    convert           convert a file into ndjson|json format
    database          perform operations with your Aito database instance

```

***NOTE:*** For database action, remember to set up your Aito instance credentials.

For addition guide of the CLI tool, see the [CLI documentations](docs/cli.md)

### Integrating with [pandas](https://pandas.pydata.org/) DataFrame

* Infer Aito Schema from a pandas DataFrame:
  ```python
  from aito.utils.schema_handler import SchemaHandler

  schema_handler = SchemaHandler()
  schema_handler.infer_table_schema_from_pandas_dataframe(df)
  ```

## Feedback & bug reports
We take our quality seriously and aim for the smoothest developer experience possible. If you run into problems, please send an email to support@aito.ai containing reproduction steps and we'll fix it as soon as possible.

## License
[MIT License](LICENSE)

## [Change logs](docs/change_logs.md)
