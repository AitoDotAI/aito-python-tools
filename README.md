# aito-python-tools

A collection of tools useful for Aito users


## Installation
`pip install aitoai` Requires python 3.6+.

## Usage

### Command line interface support tools

```bash
➜ aito -h
usage:  python aito.py [-h] <action> [<args>]
        To see help text, you can run:
            python aito.py -h
            python aito.py <action> -h

        The most commonly actions are:
            convert     convert data into ndjson format
            client      set up and do task with an aito client


positional arguments:
  action      action to perform

optional arguments:
  -h, --help  show this help message and exit
```

***NOTE:*** For client action, remember to set up your Aito instance, either through environment variable or dotenv file or using the command line arguments

Common use cases:

* Convert a csv file to ndjson.gz format for file upload and infer a [Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/):
```bash
➜ aito convert -c myInferredTableSchema.json -z csv myFile.csv myConverterFile.ndjson.gz
```
* Convert a ndjson file according to a given Aito schema:
```bash
➜ aito convert -s givenSchema.json json myFile.json myConverterFile.ndjson
```
This action will convert the data to match the given schema (e.g: Convert the "id" field from *Int* in the original data to *String*)
* Convert a file to other formats:
```bash
➜ aito convert -f csv json myFile.json myConverterFile.csv
```
* Upload entries to an existing table (a table of which [schema has been created](https://aito.ai/docs/api/#put-api-v1-schema)) in an Aito instance:
```bash
➜ aito client -u MY_AITO_INSTANCE_URL -r MY_RO_KEY -w MY_RW_KEY upload-batch myTable < myTableEntries.json
```  
***NOTE:*** This example set up the client using the command line arguments
* Upload a file to an existing table in an Aito instance:
```bash
➜ aito client -e myAitoCredentials.env upload-file myTable myFile.csv
```
***NOTE:*** This example set up the client using the environment variables stored in the dotenv file `myAitoCredentials.env`
* Upload a file to a non-existing table in a Aito instance using the inferred schema:
```bash
➜ aito client upload-file -c newTable myFile.csv
```
In other word, it creates an inferred Aito table schema from the file, create a table `newTable` with the inferred schema, and finally upload the file `myFile.csv`
***Note:*** This example set up the client using the environment variables

### Integrating with [pandas](https://pandas.pydata.org/)

* Generate Aito Schema from a pandas DataFrame:
  ```python
  from aito.schema.schema_handler import SchemaHandler

  schema_handler = SchemaHandler()
  schema_handler.generate_table_schema_from_pandas_dataframe(df)
  ```

## Feedback & bug reports
We take our quality seriously and aim for the smoothest developer experience possible. If you run into problems, please send an email to support@aito.ai containing reproduction steps and we'll fix it as soon as possible.
