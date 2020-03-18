Quickstart
==========

This section explains how to upload data to Aito with either :doc:`CLI <cli>` or :doc:`Python SDK <sdk>`.

Essentially, upload data to Aito can be broken down into the following steps:

1. Infer Aito table schema
2. Change the inferred schema if needed
3. Create a table
4. Convert the data to appropriate format for uploading
5. Upload the data

.. note::

  Skip steps 1, 2, and 3 if you upload data to an existing table

.. _cliQuickStartUploadData:

Uploading Data with CLI
-----------------------

.. note::

  You can use the :ref:`Quick Add Table Operation <cliQuickAddTable>` instead of doing upload step-by-step if
  you want to upload to a new table and don't think you need to adjust the inferred schema.


The CLI supports all steps needed to upload data:

1. :ref:`cliInferTableSchema`

  For examples, infer a table schema from a csv file::

    $ aito infer-table-schema csv < path/to/myCSVFile.csv > path/to/inferredSchema.json

2. Change the inferred schema if needed:

  You might want to change the ColumnType_, e.g: The ``id`` column should be of type ``String`` instead of ``Int``,
  or add a Analyzer_ to a ``Text`` column. In that case, just make changes to the inferred schema JSON file.

3. :ref:`cliCreateTable`

  You need a table name and a table schema to create a table::

    $ aito database create-table tableName path/to/tableSchema.json

4. :ref:`Convert the data to appropriate format for uploading <cliConvert>`

  You can either convert the data to:

    - A list of entries in JSON format for `Batch Upload`_::

        $ aito convert csv --json path/to/myCSVFile.csv > path/to/myConvertedFile.json

    - A NDJSON file for `File Upload`_::

        $ aito convert csv < path/to/myFile.csv > path/to/myConvertedFile.ndjson

      Remember to gzip the NDJSON file::

        $ gzip path/to/myConvertedFile.ndjson


  If you made change to the inferred schema or have an existing schema, use the schema when converting to make sure that the converted data matches the schema::

    $ aito convert csv -s path/to/updatedSchema.json path/to/myCSVFile.csv > path/to/myConvertedFile.ndjson

5. Upload the Data

  You can upload data with the CLI by using the :ref:`cliDatabase`.

  First, :ref:`cliSetUpAitoCredentials`. The easiest way is by using the environment variables::

    $ source AITO_INSTANCE_NAME=your-instance-name
    $ source AITO_API_KEY=your-api-key

  You can then upload the data by either:

    - :ref:`cliBatchUpload`::

        $ aito database upload-batch tableName < tableEntries.json

    - :ref:`cliFileUpload`::

        $ aito database upload-file tableName tableEntries.ndjson.gz

.. _Analyzer: https://aito.ai/docs/api/#schema-analyzer
.. _Batch Upload: https://aito.ai/docs/api/#post-api-v1-data-table-batch
.. _ColumnType: https://aito.ai/docs/api/#schema-column-type
.. _File Upload: https://aito.ai/docs/api/#post-api-v1-data-table-file