# aito-python-tools

A collection of useful tools for [Aito](https://aito.ai/) users


## Installation
`pip install aitoai` ***Requires python 3.6+.***

## Usage

### Command line interface support tools

```bash
➜ aito -h
usage:  aito [-h] <action> [<options>]
        To see help for a specific action:
            aito <action> -h

        The most commonly actions are:
            convert     convert data into ndjson format
            client      set up a client and perform CRUD operations


positional arguments:
  action      action to perform

optional arguments:
  -h, --help  show this help message and exit
```

***NOTE:*** For client action, remember to set up your Aito instance, either through environment variable or dotenv 
file or using the command line arguments

### Convert data to be uploaded into Aito:
* By default, convert takes standard input and standard ouput. To redirect: 
    ```bash
    ➜ aito convert csv < path/to/myFile.csv > path/to/myConvertedFile.ndjson
    ```
* Convert a csv file to [ndjson](http://ndjson.org/) format for 
[file upload](https://aito.ai/docs/api/#post-api-v1-data-table-file):
    ```bash
    ➜ aito convert csv path/to/myFile.csv > path/to/myConvertedFile.ndjson
    ```
* Convert an excel file to [JSON](https://www.json.org/) format for 
[batch upload](https://aito.ai/docs/api/#post-api-v1-data-table-file) 
and infer a [Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/):
    ```bash
    ➜ aito convert excel path/to/myFile.xlsx --json > path/to/myConvertedFile.json
    ```
* Convert a file and infer an [Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/) on the way:
    ```bash
    ➜ aito convert csv -c path/to/myInferredTableSchema.json path/to/myFile.csv > path/to/myConvertedFile.ndjson
    ```
* Convert a file into the desired format declared in a given 
[Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/) 
(e.g: Id should be string instead of Int):
    ```bash
    ➜ aito convert csv -s path/to/desiredSchema.json path/to/myFile.csv > path/to/myConvertedFile.ndjson
    ```
*This is useful if you want to change the created schema and convert the data accordingly*
  
### Use the client to perform CRUD operations
* ***Setting up the client***:

    There are 3 ways to set up the client:
    *  Using environment variable: You should have 3 environment variable:
        * *AITO_INSTANCE_URL*: your instance url. It should be similar to ```https://my-instance.api.aito.ai``` 
        
        ***NOTE***: No slash character(```/```) at the end of the instance url
        * *AITO_RW_KEY*: your read-write api key
        * *AITO_RO_KEY*: your read-only key (optional)
        
        Now you can execute different client tasks. For example:
        ```bash
        ➜ aito client <client-operation> ...
        ``` 
    * Using a dotenv (```.env```) file:
        * Your .env file should contain environment variables as described above. 
       You can set up the client with the dotenv file using the ```-e``` flag. For example:
        ```bash
        ➜ aito client -e path/to/myDotEnvFile.env <client-operation> ...
        ``` 
    * Using flags:
    
        You can set up the client using flags:
        ```bash
        ➜ aito client -u MY_AITO_INSTANCE_URL -r MY_READ_ONLY_API_KEY -w MY_READ_WRITE_API_KEY <client-operation> ...
        ```
         

* Create table using a [Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/):
    ```bash
    ➜ aito client -u MY_AITO_INSTANCE_URL -r MY_RO_KEY -w MY_RW_KEY upload-batch myTable < myTableEntries.json
    ```
* **Batch-upload**: Upload entries to an *existing* table 
(a table of which [schema has been created](https://aito.ai/docs/api/#put-api-v1-schema)) in an Aito instance:
    ```bash
    ➜ aito client -u MY_AITO_INSTANCE_URL -r MY_RO_KEY -w MY_RW_KEY upload-batch myTable < myTableEntries.json
    ```  instance
* **File-upload**: Upload a file to an *existing* table in an Aito instance:
    ```bash
    ➜ aito client -e myAitoCredentials.env upload-file myTable myFile.csv
    ```

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