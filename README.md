# aito-python-tools
[![PyPI](https://img.shields.io/pypi/pyversions/aitoai?style=plastic)](https://github.com/AitoDotAI/aito-python-tools) [![PyPI version](https://badge.fury.io/py/aitoai.svg)](https://badge.fury.io/py/aitoai)

A useful library for [Aito](https://aito.ai/) users containing:
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

***NOTE:*** For the database action, remember to set up your Aito instance credentials.

For an additional guide of the CLI tool, see the [CLI documentations](docs/cli.md)

### Getting your data into Aito using Python

To get you data into Aito you will have to go through the following steps:

1. [Define Aito schema](#1-define-aito-schema)
2. [Upload Aito schema](#2-upload-aito-schema)
3. [Upload data](#3-upload-data)

#### 1. Define Aito schema 

You can either create the schema manually, by using your own code or use the infer feature of the Aito python library.

You can use [pandas](https://pandas.pydata.org/) DataFrame to infer your Aito schema.

For more information on the Aito schema can be found [here](https://aito.ai/docs/articles/defining-a-database-schema/)

##### Infer Aito Schema from a pandas DataFrame
You can download the example file from [here](https://raw.githubusercontent.com/AitoDotAI/kickstart/master/reddit_sample.csv).
```python
import pandas as pd
from aito.utils.schema_handler import SchemaHandler

# Load a CSV into a pandas Dataframe
dataframe = pd.read_csv("reddit_sample.csv", delimiter=",")

# Fill empty values and replace whitespace in column names
dataframe.fillna(value="NaN",inplace=True)
dataframe.columns = dataframe.columns.str.replace(" ", "_")

schema_handler = SchemaHandler()
table_schema = schema_handler.infer_table_schema_from_pandas_data_frame(dataframe)
```
#### 2. Upload Aito schema 

To upload your Aito schema, you will need the name of your Aito instance and the read-write API key.

##### Upload Aito table schema
```python
from aito.utils.aito_client import AitoClient

aito_table_name = "your-table-name-in-aito-schema"

# Aito table schema example
table_schema = {
  "type" : "table",
  "columns": {
      "Churn": {
          "nullable": True,
          "type": "Boolean"
      },
      "CustomerID": {
          "nullable": False,
          "type": "String"
          }
      }
  }

aito_client = AitoClient(instance_name="your-aito-instance-name", api_key="your-rw-api-key")
aito_client.put_table_schema(table_name=aito_table_name, table_schema=table_schema)

# Check your table schema in Aito
aito_client.get_table_schema(table_name=aito_table_name)
```

##### Upload full Aito schema
```python
from aito.utils.aito_client import AitoClient

# Aito DB schema example
database_schema = {
  "schema": {
    "Churntable": {
      "type": "table",
      "columns": {
        "Churn": {
          "nullable": True,
          "type": "Boolean"
        },
        "CustomerID": {
          "nullable": False,
          "type": "String",
          "link": "Customertable.CustomerID"
        }
      }
    },
    "Customertable": {
      "type": "table",
      "columns": {
        "CustomerID": {
          "type": "String"
        },
        "name": {
          "type": "String"
        }
      }
    }
  }
}

aito_client = AitoClient(instance_name="your-aito-instance-name", api_key="your-rw-api-key")
aito_client.put_database_schema(database_schema=database_schema)

# Check your DB schema in Aito
aito_client.get_database_schema()
```

#### 3. Upload data

To get your data into Aito, you will first have to transform it into the JSON format. You can use the library to either upload your data by reading it into memory or by file upload (yielding is not currently supported). For file upload, the file has to be in gzip compressed newline delimited JSON format (file should end with .ndjson.gz). You will also need the name of your Aito instance and the read-write API key.

You can download the example file from [here](https://raw.githubusercontent.com/AitoDotAI/kickstart/master/reddit_sample.csv).

##### Upload data in memory
```python
import pandas as pd
from aito.utils.aito_client import AitoClient

# Define the table you want to upload data into
aito_table_name = "your-table-name-in-aito-schema"

# Load a CSV into a pandas Dataframe
dataframe = pd.read_csv("reddit_sample.csv", delimiter=",")

# Fill empty values and replace whitespace in column names
dataframe.fillna(value="NaN",inplace=True)
dataframe.columns = dataframe.columns.str.replace(" ", "_")

# Transform the pandas dataframe into a dictionary
data = dataframe.to_dict(orient="records")

# Upload the data
aito_client = AitoClient(instance_name="your-aito-instance-name", api_key="your-rw-api-key")
aito_client.populate_table_entries(table_name=aito_table_name, entries=data)

# Check the data
aito_client.query_table_entries(table_name=aito_table_name, limit=2)
```

##### Upload gzipped ndjson file
```python
import pandas as pd
import gzip
import ndjson
from pathlib import Path
from aito.utils.aito_client import AitoClient

# Load a CSV into a pandas Dataframe
dataframe = pd.read_csv("reddit_sample.csv", delimiter=",")

# Fill empty values and replace whitespace in column names
dataframe.fillna(value="NaN",inplace=True)
dataframe.columns = dataframe.columns.str.replace(" ", "_")

# Transform the pandas dataframe into the ndjson format
data = dataframe.to_dict(orient="records")
output_ndjson = ndjson.dumps(data)
output_ndjson = output_ndjson.encode('utf-8')

# Write gzipped ndjson
with gzip.GzipFile("reddit_sample.ndjson.gz", "w") as output:
  output.write(output_ndjson)

# Define the table you want to upload data into
aito_table_name = "your-table-name-in-aito-schema"

file_path = Path("reddit_sample.ndjson.gz")

# Upload the data
aito_client = AitoClient(instance_name="your-aito-instance-name", api_key="your-rw-api-key")
aito_client.populate_table_by_file_upload(table_name=aito_table_name, file_path=file_path)

# Check the data
aito_client.query_table_entries(table_name=aito_table_name, limit=2)
```

### Delete data

To delete data from Aito you will need the name of your Aito instance and the read-write API key. You can either delete data per table or delete the whole database.

#### Delete table
```python
from aito.utils.aito_client import AitoClient

aito_table_name = "your-table-name-in-aito-schema"

aito_client = AitoClient(instance_name="your-aito-instance-name", api_key="your-rw-api-key")
aito_client.delete_table(table_name=aito_table_name)
```
#### Delete entire database
```python
from aito.utils.aito_client import AitoClient

aito_client = AitoClient(instance_name="your-aito-instance-name", api_key="your-rw-api-key")
aito_client.delete_database()
```

## Feedback & bug reports
We take our quality seriously and aim for the smoothest developer experience possible. If you run into problems, please send an email to support@aito.ai containing reproduction steps and we'll fix it as soon as possible.

## License
[MIT License](LICENSE)

## [Change logs](docs/change_logs.md)
