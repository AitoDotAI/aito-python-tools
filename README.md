# aito-python-tools

A collection of useful tools for [Aito](https://aito.ai/) users


## Installation
`pip install aitoai` ***Requires python 3.6+.***

## Usage

### Command line interface support tools

```bash
➜ aito -h
usage:  aito [-h] <action> [<args>]
        To see help text, you can run:
            aito -h
            aito <action> -h

        The most commonly actions are:
            convert     convert data into ndjson format
            client      set up and do task with an aito client


positional arguments:
  action      action to perform

optional arguments:
  -h, --help  show this help message and exit
```

***NOTE:*** For client action, remember to set up your Aito instance, either through environment variable or dotenv 
file or using the command line arguments

### Common use cases:
``
#### Convert:
* Convert a csv file to [ndjson](http://ndjson.org/) format for 
[file upload](https://aito.ai/docs/api/#post-api-v1-data-table-file):
    ```bash
    ➜ aito convert csv myFile.csv -c myInferredTableSchema.json > myConvertedFile.ndjson
    ```
* Convert an excel file to [JSON](https://www.json.org/) format for 
[batch upload](https://aito.ai/docs/api/#post-api-v1-data-table-file) 
and infer a [Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/):
    ```bash
    ➜ aito convert excel myFile.xlsx --json > myConvertedFile.json
    ```
* Convert a file and infer an [Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/) on the way:
    ```bash
    ➜ aito convert csv myFile.csv -c myInferredTableSchema.json  > myConvertedFile.ndjson
    ```
* Convert a file into the desired format declared in a given 
[Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/) 
(e.g: Id should be string instead of Int):
    ```bash
    ➜ aito convert csv myFile.csv -s desiredSchema.json > myConvertedFile.ndjson
    ```
*This is useful if you want to change the created schema and convert the data accordingly*
* Convert using standard input
    ```bash
    ➜ aito convert csv < myFile.csv > myConvertedFile.ndjson
    ```
  
#### Client
* **Batch-upload**: Upload entries to an *existing* table 
(a table of which [schema has been created](https://aito.ai/docs/api/#put-api-v1-schema)) in an Aito instance:
    ```bash
    ➜ aito client -u MY_AITO_INSTANCE_URL -r MY_RO_KEY -w MY_RW_KEY upload-batch myTable < myTableEntries.json
    ```  
    ***NOTE:*** This example set up the client using the command line arguments
* **File-upload**: Upload a file to an *existing* table in an Aito instance:
    ```bash
    ➜ aito client -e myAitoCredentials.env upload-file myTable myFile.csv
    ```
    ***NOTE:*** This example set up the client using the environment variables stored in 
    the dotenv file `myAitoCredentials.env`

### Integrating with [pandas](https://pandas.pydata.org/)

* Generate Aito Schema from a pandas DataFrame:
  ```python
  from aito.schema.schema_handler import SchemaHandler

  schema_handler = SchemaHandler()
  schema_handler.generate_table_schema_from_pandas_dataframe(df)
  ```

## Feedback & bug reports
We take our quality seriously and aim for the smoothest developer experience possible. If you run into problems, please send an email to support@aito.ai containing reproduction steps and we'll fix it as soon as possible.

## License
[MIT License](LICENSE)