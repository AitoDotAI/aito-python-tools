Aito CLI
===========================

The Aito Command Line Interface (Aito CLI) is an open source tool that enables you to interact with
your Aito instance using commands in your command-line shell with minimum setup.

Quickstart
----------

To get started:

  .. code-block:: console

    $ aito -h
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

Upload a Data File
------------------

The easiest way to upload a file to your Aito database instance is by using the ``database quick-add-table``

1. `Set up the credentials`_
2. Run::

    $ aito database quick-add-table path/to/dataFile

  To see help::

    $ aito database quick-add-table -h


Quick add table essentially does the following step:

1. `Infer an Aito table schema`_
2. :ref:`Create a table using the inferred schema<Create a table>`
3. :ref:`Convert the file to gzipped NDJSON file for file upload<Convert data>`
4. :ref:`Upload the file to the created table<File upload>`


You can customized this workflow, for example, adjust the inferred schema before creating table or
upload file to an existing table, by following each step guide and execute it manually.

Infer an Aito Table Schema
--------------------------

Infer an Aito table schema from the input data

Supported input formats:

- csv
- excel (both xls and xlsx)
- JSON_
- NDJSON_

Commands:

- To see help::

    $ aito infer-table-schema -h

  To see help for a specific input format::

    $ aito infer-table-schema <input-format> -h

- By default, the command takes standard input and standard output. To redirect::

    $ aito infer-table-schema csv < path/to/myFile.csv > path/to/schemaFile.json

- Infer table schema from a csv file::

    $ aito infer-table-schema csv < path/to/myCSVFile.csv

- Infer table schema from a semicolon delimited csv file::

    $ aito infer-table-schema csv -d ';' < path/to/myCSVFile.csv

- Infer table schema from a semicolon delimited comma decimal point csv file::

    $ aito infer-table-schema csv -d ';' -p ',' < path/to/myCSVFile.csv

- Infer table schema from an excel file::

    $ aito infer-table-schema excel path/to/myExcelFile.xlsx

- Infer table schema from a single sheet of an excel file::

    $ aito infer-table-schema excel -o sheetName path/to/myExcelFile.xls

- Infer table schema from a JSON_ file::

    $ aito infer-table-schema json path/to/myJsonFile.json

- Infer table schema from a NDJSON_ file::

    $ aito infer-table-schema ndjson path/to/myNdJsonFile.ndjson


Convert Data
------------

Aito takes JSON array of objects for `Inserting multiple
entries <https://aito.ai/docs/api/#post-api-v1-data-table-batch>`__ and
a gzip compressed NDJSON_ file for
`File upload <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__.

The convert action helps you to convert your data file into JSON_ or NDJSON_ format.

Supported input formats:

- csv
- excel (both xls and xlsx)
- JSON_
- NDJSON_

Commands:

- To see help::

    $ aito convert -h

  To see help for a specific input format::

    $ aito convert <input-format> -h

- By default, the command takes standard input and standard output. To redirect::

    $ aito convert csv < path/to/myFile.csv > path/to/myConvertedFile.ndjson

- Convert a csv file to NDJSON_ format for file upload::

    $ aito convert csv path/to/myFile.csv > path/to/myConvertedFile.ndjson

- Convert an excel file to JSON_ format for batch upload::

    $ aito convert excel --json path/to/myFile.xlsx > path/to/myConvertedFile.json

- Convert a csv file to NDJSON_ format and and infer the file table schema on the way::

    $ aito convert csv -c path/to/inferredTableSchema.json path/to/myFile.csv > path/to/myConvertedFile.ndjson

- Convert a file and use a given Aito table schema. This function is useful when want to make changes to the inferred schema and want to convert the data accordingly. For example, the `id` column should be of String type instead of Int type::

    $ aito convert csv -s path/to/desiredSchema.json path/to/myFile.csv > path/to/myConvertedFile.ndjson

Perform Database Operations
---------------------------

.. _setUpAitoCredentials:

Set Up the Credentials
~~~~~~~~~~~~~~~~~~~~~~

Performing operation with your Aito database instance always requires credentials.

There are 3 ways to set up the credentials:

1. The most convenient way is to set up the following environment variables::

    $ source AITO_INSTANCE_NAME=your-instance-name
    $ source AITO_API_KEY=your-api-key

  You can now perform operations::

    $ aito database <operation> ...

2. Using a dotenv (``.env``) file

  Your .env file should contain environment variables as described above.

  You can set up the credentials using a dotenv file with the ``-e`` flag::

    $ aito database -e path/to/myDotEnvFile.env <operation> ...

3. Using flags:

  You can set up the credentials using ``-i`` flag for the instance name and ``-k`` flag for the api key::

    $ aito database -i MY_AITO_INSTANCE_NAME -k MY_API_KEY <operation> ...

Database Operations
~~~~~~~~~~~~~~~~~~~

.. note::

  All of the following operations require read-write key

Quick Add a Table
^^^^^^^^^^^^^^^^^
Infer a table schema based on the given file, create a table using the file name and upload the file content to the created table::

    $aito database quick-add-table path/to/tableEntries.json

Create a Table
^^^^^^^^^^^^^^
Create a table using the given Aito table schema::

    $ aito database create-table tableName path/to/tableSchema.json

Batch Upload
^^^^^^^^^^^^

Upload entries to an *existing* table (a table of which `schema has been created <https://aito.ai/docs/api/#put-api-v1-schema>`_) in your Aito instance::

    $ aito database upload-batch tableName < tableEntries.json


File Upload
^^^^^^^^^^^

Upload a file to an *existing* table in your Aito instance::

    $ aito database upload-file tableName tableEntries.csv

Delete a Table
^^^^^^^^^^^^^^

Delete a table schema and all the data inside it:

  .. code-block:: console

    $ aito database delete-table tableName

  .. warning:: This operation is irreversible

Delete the Whole Database
^^^^^^^^^^^^^^^^^^^^^^^^^

Delete all tables schema and all data in the instance:

  .. code-block:: console

    $ aito database delete-database

  .. warning:: This operation is irreversible

Tab Completion
~~~~~~~~~~~~~~~

The CLI supports tab completion using argcomplete_

-  To activate global completion::

    $ activate-global-python-argcomplete

-  If you choose not to use global completion::

    $ eval "$(register-python-argcomplete aito)"

- You might have to install ``python3-argcomplete``::

    $ sudo apt install python3-argcomplete

- Please refer the `argcomplete documentation`_


Integration with SQL Database
-----------------------------
Aito supports integration with your SQL database. To enable this feature, please follow the instructions
:doc:`here <sql>`

.. _NDJSON: http://ndjson.org/
.. _JSON: https://www.json.org/
.. _argcomplete: https://argcomplete.readthedocs.io/en/latest/
.. _argcomplete documentation: https://argcomplete.readthedocs.io/en/latest/#activating-global-completion