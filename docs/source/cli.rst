Aito CLI
========

The Aito Command Line Interface (Aito CLI) is an open source tool that enables you to interact with
your Aito instance using commands in your command-line shell with minimum setup.

To get started:

  .. code-block:: console

    $ aito -h
    usage: aito [-h] [-V] [-v] [-q]

    optional arguments:
      -h, --help     show this help message and exit
      -V, --version  display the version of this tool
      -v, --verbose  display verbose messages
      -q, --quiet    display only error messages

    To see all available commands, you can run:
      aito list

    To see the help text, you can run:
      aito -h
      aito <command> -h
      aito <command> <subcommand> -h


:ref:`Quickstart guide to upload data <cliQuickStartUploadData>`


.. _cliSetUpAitoCredentials:

Set Up Aito Credentials
~~~~~~~~~~~~~~~~~~~~~~~

Performing operations with your Aito database instance always requires credentials.

There are 3 ways to set up the credentials:

1. The most convenient way is to use the ``configure`` command::

    $ aito configure

  When you enter this command, the CLI prompts you for two pieces of information:

    - Your Aito instance URL
    - Your Aito API key

  The CLI stores this information in a profile named `default` in the credentials file. By default, the information in
this profile is used when you run a command that requires credentials.

  The credentials file is stored in `$HOME/.config/aito/credentials` (`%UserProfile%` in Windows). You can manually edit this credentials file.

  You can also specify the named profile with the ``--profile`` flag and use the named profile for commands that require credentials.

2. Set up the following environment variables::

    $ export AITO_INSTANCE_URL=your-instance-url
    $ export AITO_API_KEY=your-api-key

  You can now perform commands::

    $ aito <command> ...

3. Using flags:

  You can set up the credentials using ``-i`` flag for the instance url and ``-k`` flag for the api key::

    $ aito -i MY_AITO_INSTANCE_URL -k MY_API_KEY <command> ...

.. _cliQuickAddTable:

Quick Add a Table
~~~~~~~~~~~~~~~~~

Infer a table schema based on the given file, create a table using the file name and upload the file content to the created table::

  $ aito quick-add-table path/to/tableEntries.json

.. _cliCreateTable:

Create a Table
~~~~~~~~~~~~~~

Create a table using the given Aito table schema::

  $ aito create-table tableName path/to/tableSchema.json

.. _cliBatchUpload:


Delete a Table
~~~~~~~~~~~~~~

Delete a table schema and all the data inside it:

  .. code-block:: console

    $ aito delete-table tableName

  .. warning:: This operation is irreversible

Delete the Whole Database
~~~~~~~~~~~~~~~~~~~~~~~~~

Delete the database's schema and all data in the instance:

  .. code-block:: console

    $ aito delete-database

  .. warning:: This operation is irreversible

Upload Entries to a Table
~~~~~~~~~~~~~~~~~~~~~~~~~

Upload entries to an *existing* table (a table of which `schema has been created <https://aito.ai/docs/api/#put-api-v1-schema>`_) in your Aito instance::

    $ aito upload-entries tableName < tableEntries.json

.. _cliFileUpload:

Upload a File to a Table
~~~~~~~~~~~~~~~~~~~~~~~~

Upload a file to an *existing* table in your Aito instance::

    $ aito upload-file tableName tableEntries.ndjson.gz

.. _cliInferTableSchema:

infer-table-schema command
--------------------------

The ``infer-table-schema`` command helps you to infer a table schema from the input data

Supported input formats:

- csv
- excel (both xls and xlsx)
- JSON_
- NDJSON_

Infer table schema usage
~~~~~~~~~~~~~~~~~~~~~~~~

To see help::

  $ aito infer-table-schema -h

The command supports different object for each input format, for instance, specifying the delimiter for csv format.
To see help for a specific input format::

  $ aito infer-table-schema <input-format> -h


Infer table schema redirection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, the command takes standard input and standard output. To redirect::

  $ aito infer-table-schema csv < path/to/myFile.csv > path/to/schemaFile.json

Infer Table Schema From Csv
~~~~~~~~~~~~~~~~~~~~~~~~~~~

To see help::

  $ aito infer-table-schema csv -h

Example::

  $ aito infer-table-schema csv < path/to/myCSVFile.csv

Infer a table schema from a semicolon delimited csv file::

  $ aito infer-table-schema csv -d ';' < path/to/myCSVFile.csv

Infer a table schema from a semicolon delimited comma decimal point csv file::

  $ aito infer-table-schema csv -d ';' -p ',' < path/to/myCSVFile.csv

Infer Table Schema From Excel
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To see help::

  $ aito infer-table-schema excel -h

Example::

  $ aito infer-table-schema excel path/to/myExcelFile.xlsx

.. note::

  Infer table schema from Excel only take file path instead of stdin

Infer a table schema from a single sheet of an excel file::

  $ aito infer-table-schema excel -o sheetName path/to/myExcelFile.xls


Infer Table Schema From JSON_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To see help::

  $ aito infer-table-schema json -h

Example::

  $ aito infer-table-schema json path/to/myJSONFile.json


Infer Table Schema From NDJSON_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To see help::

  $ aito infer-table-schema ndjson -h

Example::

  $ aito infer-table-schema ndjson path/to/myNDJSONFile.ndjson


.. _cliConvert:

convert command
---------------

The ``convert`` command helps you to convert the input data into JSON_ or NDJSON_ for upload or convert the data
according to a table schema.

Aito takes JSON array of objects for `Inserting multiple
entries <https://aito.ai/docs/api/#post-api-v1-data-table-batch>`__ and
a gzip compressed NDJSON_ file for
`File upload <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__.

Supported input formats:

- csv
- excel (both xls and xlsx)
- JSON_
- NDJSON_

Convert usage
~~~~~~~~~~~~~

To see help::

  $ aito convert -h

The command supports different object for each input format, for instance, specifying the delimiter for csv format.
To see help for a specific input format::

  $ aito convert <input-format> -h

Convert redirection
~~~~~~~~~~~~~~~~~~~

By default, the command takes standard input and standard output. To redirect::

  $ aito convert csv < path/to/myFile.csv > path/to/myConvertedFile.ndjson

Convert to JSON
~~~~~~~~~~~~~~~

By default, the command converts the input to the NDJSON_ format. If you want to convert to the JSON_ format, use the ``-j`` or ``--json`` flag::

  $ aito convert <input-format> --json ...

Convert and Infer Schema
~~~~~~~~~~~~~~~~~~~~~~~~

You can convert the data and infer a table schema at the same time by using the ``-c`` or ``--create-table-schema`` flag::

  $ aito convert <input-format> -c path/to/inferredTableSchema.json ...

Convert Using A Table Schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can convert the data to match an existing table schema by using the ``-s`` or ``--use-table-schema`` flag::

  $ aito convert <input-format> -s path/to/tableSchema.json ...

Convert CSV
~~~~~~~~~~~

To see help::

  $ aito convert csv -h

Example::

  $ aito convert csv path/to/myCsvFile.csv > path/to/convertedFile.ndjson

Convert a semicolon delimited csv file::

  $ aito convert csv -d ';' < path/to/myCsvFile.csv

Convert a semicolon delimited comma decimal point csv file::

  $ aito convert csv -d ';' -p ',' < path/to/myCsvFile.csv

Convert Excel
~~~~~~~~~~~~~

To see help::

  $ aito convert excel -h

Example::

  $ aito convert excel path/to/myExcelFile.xlsx > path/to/convertedFile.ndjson

.. note::

  Convert from Excel only takes a file path instead of stdin

Convert a single sheet of an excel file::

  $ aito convert excel -o sheetName path/to/myExcelFile.xls

Convert JSON
~~~~~~~~~~~~

To see help::

  $ aito convert json -h

Example::

  $ aito convert json < path/to/myJSONFile.json > path/to/convertedFile.ndjson

Convert NDJSON
~~~~~~~~~~~~~~

To see help::

  $ aito convert ndjson -h

Example::

  $ aito convert ndjson -j < path/to/myNDJSONFile.ndjson > path/to/convertedFile.json



Tab Completion
--------------

The CLI supports tab completion using argcomplete_

-  Activate global completion::

    $ activate-global-python-argcomplete

-  If you don't want to use global completion::

    $ eval "$(register-python-argcomplete aito)"

- You might have to install ``python3-argcomplete``::

    $ sudo apt install python3-argcomplete

- Please refer to the `argcomplete documentation`_


Integration with SQL Database
-----------------------------
Aito supports integration with your SQL database. To enable this feature, please follow the instructions
:doc:`here <sql>`

.. _NDJSON: http://ndjson.org/
.. _JSON: https://www.json.org/
.. _argcomplete: https://argcomplete.readthedocs.io/en/latest/
.. _argcomplete documentation: https://argcomplete.readthedocs.io/en/latest/#activating-global-completion
