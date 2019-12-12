# Aito Command Line Interface

_DISCLAIMER: Aito Command Line Interface repository is experimental code that we are already using ourselves. It might be a bit rough on the edges and is not yet ready for production grade release. We are constantly developing it and changes are likely. Feel free to use, and share any feedback with us._

The Aito CLI tool aims to help you get upload your data to your Aito instance as fast as possible.

## Installation

To install with pip, run: `pip install aitoai`

To install from source, first clone the repository and then run: `python setup.py install`

Aito supports integration with your SQL database. To enable this feature, please follow the instructions [here](sql_functions.md/#installation)

## Introduction

To get started:

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

Supported actions:
* [***quick-add-table***](#quick-add-table)
* [***infer-table-schema***](#infer-table-schema)
* [***convert***](#convert)
* [***database***](#database)

## <a name="quick-add-table"> Quick guide to upload the data
Quickly upload a file to your database instance by following these steps:
* [Set up the credentials](#set-up-credentials)
* Run:
  ```bash
  aito database quick-add-table path/to/dataFile
  ```
  To see help:
  ```bash
  aito database quick-add-table -h
  ```

## Step-by-step instructions to upload the data
* [Infer](#infer-table-schema) an [Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/) from your data file:
  ```bash
  aito infer-table-schema <file-format> path/to/yourFile path/to/inferedSchema.json
  ```
  Examine the inferred schema and change if necessary.
  E.g: An 'id' column should be of type 'String' but is inferred as 'Int'.

  ***You can skip this step if you already have an existing table or a table schema.***

* [Set up the credentials](#set-up-credentials)
* Create a table with the created schema:
  ```bash
  aito database create-table <table-name> path/to/tableSchema
  ```
  ***You can skip this step if you want to upload the data to an existing table***
* Upload the data file to your database instance:
  ```bash
  aito database upload-file <table-name> path/to/dataFile
  ```

## In-depth guide
### <a name="infer-table-schema"> Infer an Aito table schema from a file
* To see help:
  ```bash
  aito infer-table-schema - h
  ```
* By default, the command takes standard input and standard output. To redirect:
  ```bash
  aito infer-table-schema csv < path/to/myFile.csv > path/to/schemaFile.json
  ```
* Infer table schema from a csv file
  ```bash
  aito infer-table-schema csv < path/to/myCSVFile.csv
  ```
  Infer table schema from a semicolon delimited csv file
  ```bash
  aito infer-table-schema csv -d ';' < path/to/myCSVFile.csv
  ```
  Infer table schema from a semicolon delimited comma decimal point csv file
  ```bash
  aito infer-table-schema csv -d ';' -p ',' < path/to/myCSVFile.csv
  ```
* Infer table schema from an excel file:
  ```bash
  aito infer-table-schema excel path/to/myExcelFile.xlsx
  ```
  Infer table schema from a single sheet of an excel file
  ```bash
  aito infer-table-schema excel -o sheetName path/to/myExcelFile.xls
  ```
* Infer table schema from a json file:
  ```bash
  aito infer-table-schema json path/to/myJsonFile.json
  ```
* Infer table schema from a njson file:
  ```bash
  aito infer-table-schema ndjson path/to/myNdJsonFile.ndjson
  ```

### <a name="convert"> Convert data to be uploaded into Aito
Aito takes JSON array of objects for [Inserting multiple entries](https://aito.ai/docs/api/#post-api-v1-data-table-batch) and
a gzip compressed ndjson file for [File upload](https://aito.ai/docs/api/#post-api-v1-data-table-file).
* To see help:
  ```bash
  aito convert -h
  ```
* By default, the command takes standard input and standard output. To redirect:
  ```bash
  aito convert csv < path/to/myFile.csv > path/to/myConvertedFile.ndjson
  ```
* Convert a csv file to [ndjson](http://ndjson.org/) format for
[file upload](https://aito.ai/docs/api/#post-api-v1-data-table-file):
  ```bash
  aito convert csv path/to/myFile.csv > path/to/myConvertedFile.ndjson
  ```
* Convert an excel file to [JSON](https://www.json.org/) format for
[batch upload](https://aito.ai/docs/api/#post-api-v1-data-table-file)
and infer a [Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/):
  ```bash
  aito convert excel path/to/myFile.xlsx > path/to/myConvertedFile.json
  ```
* Convert a file and infer an [Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/) on the way:
  ```bash
  aito convert csv -c path/to/myInferredTableSchema.json path/to/myFile.csv > path/to/myConvertedFile.ndjson
  ```
* Convert a file into the desired format declared in a given
[Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/)
(e.g: Id should be string instead of Int):
  ```bash
  aito convert csv -s path/to/desiredSchema.json path/to/myFile.csv > path/to/myConvertedFile.ndjson
  ```
  *This is useful if you change the inferred schema and want to convert the data accordingly*

### <a name="database"> Perform operations with your Aito database instance
#### <a name="set-up-credentials"> Setting up the credentials
Performing operation with your Aito database instance always requires credentials.
There are 3 ways to set up the credentials:
* The most convinient way is to set up the following environment variables:
  ```
  AITO_INSTANCE_NAME=your-instance-name
  AITO_RW_KEY=your read-write api key
  AITO_RO_KEY=your read-only key (optional)
  ```

  You can now perform operations with:
  ```bash
  aito database <operation> ...
  ```
* Using a dotenv (```.env```) file:

  *Your .env file should contain environment variables as described above.*

  You can set up the credentials using a dotenv file with the `-e` flag. For example:

  ```bash
  aito database -e path/to/myDotEnvFile.env <operation> ...
  ```
* Using flags:

  You can set up the credentials using `-i` flag for the instance name, `-r` flag for the read-only key, and `-w` flag for the read-write key:
  ```bash
  aito database -i MY_AITO_INSTANCE_NAME -r MY_READ_ONLY_API_KEY -w MY_READ_WRITE_API_KEY <operation> ...
  ```

#### Some common operations
* **Quick add a table**: Using a file to create a table and upload the file content to the table:

  ***Note***: Requires read-write key and system write permission (for schema creation and changing the file format for file-upload)

  ```bash
  aito database quick-add-table path/to/tableEntries.json
  ```
* **Create a table** using a [Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/):
  ```bash
  aito database create-table tableName path/to/tableSchema.json
  ```
* **Batch-upload**: Upload entries to an *existing* table
(a table of which [schema has been created](https://aito.ai/docs/api/#put-api-v1-schema)) in your Aito instance:
  ```bash
  aito database upload-batch tableName < tableEntries.json
  ```
* **File-upload**: Upload a file to an *existing* table in your Aito instance:

  ***Note***: Might requires system write permission for changing the file format for file-upload

  ```bash
  aito database upload-file tableName tableEntries.csv
  ```
* **Delete a table**:

  ***Note***: Requires read-write key
  ```bash
  aito database delete-table tableName
  ```
* **Delete the whole database**:

  ***Note***: Requires read-write key
  ```bash
  aito database delete-database
  ```

### <a name="tab-completion"> Tab Completion
The CLI supports tab completion using [argcomplete](https://argcomplete.readthedocs.io/en/latest/)

* To activate global completion:
  ```bash
  activate-global-python-argcomplete
  ```
More instructions can be found [here](https://argcomplete.readthedocs.io/en/latest/#activating-global-completion)

* If you choose not to use global completion:
  ```bash
  eval "$(register-python-argcomplete aito)"
  ```
  You might have to install `python3-argcomplete`:
  ```bash
  sudo apt install python3-argcomplete
  ```
