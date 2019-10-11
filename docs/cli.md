# Aito Command Line Interface

The aito CLI tool aims to help you get upload your data to Aito instance as fast as possible

## Installation

To install with pip, run: `pip install aitoai` 

To install from source, first clone the repository and then run: `python setup.py install`

## Introduction

To get started, simply;

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

## Quick guide to upload the data

If you just want to upload a file to your instance, follow these step:
* [Set up the client credentials](#set-up-client)
* Run:
    ```bash
    ➜ aito client quick-add-table path/to/dataFile
    ```
  
## Step-by-step to upload the data
1. Infer the [Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/) from your data file:
    ```bash
    ➜ aito infer-table-schema <file-format> path/to/yourFile path/to/inferedSchema.json
    ```
2. Examine the inferred schema and change if necessary. E.g: Field 'id' should be of type 'String' instead of type 'Int'

    ***You can skip this step if you already have the table schema.***
3. [Set up the client credentials](#set-up-client)
4. Create a table with the created schema:
    ```bash
    ➜ aito client create-table <table-name> path/to/tableSchema
    ```
   ***You can skip this step if you want to upload the data to an existing table*** 
5. Upload the data file to your instance:
    ```bash
    ➜ aito client upload-file <table-name> path/to/dataFile
    ```

## In-depth guide
### Infer table schema from file
* By default, the command takes standard input and standard output. To redirect:
    ```bash
    ➜ aito infer-table-schema csv < path/to/myFile.csv > path/to/schemaFile.json
    ```
* Infer table schema from a csv file
    ```bash
    ➜ aito infer-table-schema csv < path/to/myCSVFile.csv
    ```
    Infer table schema from a semicolon delimited csv file
    ```bash
    ➜ aito infer-table-schema csv -d ';' < path/to/myCSVFile.csv
    ```
    Infer table schema from a semicolon delimited comma decimal point csv file
    ```bash
    ➜ aito infer-table-schema csv -d ';' -p ',' < path/to/myCSVFile.csv
    ```
* Infer table schema from an excel file:
    ```bash
    ➜ aito infer-table-schema excel path/to/myExcelFile.xslx
    ```
    Infer table schema from a single sheet of an excel file 
    ```bash
    ➜ aito infer-table-schema excel path/to/myExcelFile.xls -o sheetName
    ```
* Infer table schema from a json file:
    ```bash
    ➜ aito infer-table-schema json path/to/myJsonFile.json
    ```
  
* Infer table schema from a njson file:
    ```bash
    ➜ aito infer-table-schema ndjson path/to/myNdJsonFile.ndjson
    ```

### Convert data to be uploaded into Aito:
* By default, the command takes standard input and standard output. To redirect: 
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
    ➜ aito convert csv path/to/myFile.csv -c path/to/myInferredTableSchema.json > path/to/myConvertedFile.ndjson
    ```
* Convert a file into the desired format declared in a given 
[Aito table schema](https://aito.ai/docs/articles/defining-a-database-schema/) 
(e.g: Id should be string instead of Int):
    ```bash
    ➜ aito convert csv path/to/myFile.csv -s path/to/desiredSchema.json > path/to/myConvertedFile.ndjson
    ```
*This is useful if you want to change the created schema and convert the data accordingly*
  
### Use the client to perform CRUD operations
* [***Setting up the client credentials***](#set-up-client): There are 3 ways to set up the client credentials:
    *  Using environment variable: You should set up 3 environment variable:
        ```
        AITO_INSTANCE_URL=https://your-instance.api.aito.ai
        AITO_RW_KEY=your read-write api key
        AITO_RO_KEY=your read-only key (optional)
        ``` 
        ***NOTE***: Your instance url should not end with the slash character(```/```)
        Now you can execute different client operations:
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
    ```
* **File-upload**: Upload a file to an *existing* table in an Aito instance:
    ```bash
    ➜ aito client -e myAitoCredentials.env upload-file myTable myFile.csv
    ```