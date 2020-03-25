Quickstart
==========

This section explains how to upload data to Aito with either :doc:`CLI <cli>` or :doc:`Python SDK <sdk>`.

Essentially, uploading data into Aito can be broken down into the following steps:

1. Infer a Table Schema :ref:`cli <cliQuickstartInferTableSchema>` | :ref:`sdk <sdkQuickstartInferTableSchema>`
2. Change the inferred schema if needed :ref:`cli <cliQuickstartChangeSchema>` | :ref:`sdk <sdkQuickstartChangeSchema>`
3. Create a table :ref:`cli <cliQuickstartCreateTable>` | :ref:`sdk <sdkQuickstartCreateTable>`
4. Convert the data :ref:`cli <cliQuickstartConvertData>` | :ref:`sdk <sdkQuickstartConvertData>`
5. Upload the data :ref:`cli <cliQuickstartUploadData>` | :ref:`sdk <sdkQuickstartUploadData>`

.. note::

  Skip steps 1, 2, and 3 if you upload data to an existing table
  Skip step 4 if you already have the data in the appropriate format for uploading or the data matches the table schema

If you don't have a data file, you can download our `example file <https://raw.githubusercontent.com/AitoDotAI/kickstart/master/reddit_sample.csv>`_ and follow the guide.

Upload Data with the CLI
------------------------

.. note::

  You can use the :ref:`Quick Add Table Operation <cliQuickAddTable>` instead of doing upload step-by-step if
  you want to upload to a new table and don't think you need to adjust the inferred schema.


The CLI supports all steps needed to upload data:

.. _cliQuickstartInferTableSchema:

:ref:`Infer a Table Schema <cliInferTableSchema>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For examples, infer a table schema from a csv file::

  $ aito infer-table-schema csv < path/to/myCSVFile.csv > path/to/inferredSchema.json

.. _cliQuickstartChangeSchema:

Change the Schema
~~~~~~~~~~~~~~~~~

You might want to change the ColumnType_, e.g: The ``id`` column should be of type ``String`` instead of ``Int``,
or add an Analyzer_ to a ``Text`` column. In that case, just make changes to the inferred schema JSON file.

The example below use `jq <https://stedolan.github.io/jq/>`_ to change the ``id`` column type::

  $ jq '.columns.id.type = "String"' < path/to/schemaFile.json > path/to/updatedSchemaFile.json

.. _cliQuickstartCreateTable:

:ref:`Create a Table <cliCreateTable>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You need a table name and a table schema to create a table::

  $ aito database create-table tableName path/to/tableSchema.json

.. _cliQuickstartConvertData:

:ref:`Convert the Data <cliConvert>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you made changes to the inferred schema or have an existing schema, use the schema when with the ``-s`` flag to make sure that the converted data matches the schema::

  $ aito convert csv -s path/to/updatedSchema.json path/to/myCSVFile.csv > path/to/myConvertedFile.ndjson

You can either convert the data to:

  - A list of entries in JSON format for `Batch Upload`_::

      $ aito convert csv --json path/to/myCSVFile.csv > path/to/myConvertedFile.json

  - A NDJSON file for `File Upload`_::

      $ aito convert csv < path/to/myFile.csv > path/to/myConvertedFile.ndjson

    Remember to gzip the NDJSON file::

      $ gzip path/to/myConvertedFile.ndjson


.. _cliQuickstartUploadData:

Upload the Data
~~~~~~~~~~~~~~~

  You can upload data with the CLI by using the :ref:`cliDatabase`.

  First, :ref:`cliSetUpAitoCredentials`. The easiest way is by using the environment variables::

    $ export AITO_INSTANCE_URL=your-instance-url
    $ export AITO_API_KEY=your-api-key

  You can then upload the data by either:

    - :ref:`cliBatchUpload`::

        $ aito database upload-batch tableName < tableEntries.json

    - :ref:`cliFileUpload`::

        $ aito database upload-file tableName tableEntries.ndjson.gz


Upload Data with the SDK
------------------------

The Aito Python SDK uses `Pandas DataFrame`_ for multiple operations.

The example below show how you can load a csv file into a DataFrame, please read the `official guide <https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html>`__ for further instructions.

.. code:: python

  import pandas as pd

  reddit_df = pd.read_csv('reddit_sample.csv')

.. _sdkQuickstartInferTableSchema:

:ref:`Infer a Table Schema <sdkInferTableSchema>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :ref:`apiSchemaHandler` can infer table schema from a DataFrame:

  .. code:: python

    from aito.utils.schema_handler import SchemaHandler
    schema_handler = SchemaHandler()
    inferred_schema = schema_handler.infer_table_schema_from_pandas_data_frame(data_frame)

.. _sdkQuickstartChangeSchema:

Change the Schema
~~~~~~~~~~~~~~~~~

You might want to change the ColumnType_, e.g: The ``id`` column should be of type ``String`` instead of ``Int``,
or add a Analyzer_ to a ``Text`` column.

The return inferred schema from :ref:`apiSchemaHandler` is a `Python Dictionary Object`_ and hence, can be updated by updating the value:

  .. code :: python

    inferred_schema['columns']['id']['type'] = 'String'

.. _sdkQuickstartCreateTable:

:ref:`Create a Table <sdkCreateTable>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :ref:`apiAitoClient` can create a table using a table name and a table schema:

  .. code:: python

    from aito.utils.aito_client import AitoClient
    table_schema = {
      "type": "table",
      "columns": {
        "id": { "type": "Int" },
        "name": { "type": "String" },
        "price": { "type": "Decimal" },
        "description": { "type": "Text", "analyzer": "English" }
      }
    }
    aito_client = AitoClient(instance_url='your_aito_instance_url', api_key='your_rw_api_key')
    aito_client.put_table_schema(table_name='your-table-name', table_schema=table_schema)

.. _sdkQuickstartConvertData:

Convert the Data
~~~~~~~~~~~~~~~~

The DataFrameHandler can convert a DataFrame to match an existing schema:

  .. code:: python

    converted_data_frame = data_frame_handler.convert_df_from_aito_table_schema(
      df=data_frame,
      table_schema=table_schema_content
    )

A DataFrame can be converted to:

  - A list of entries in JSON format for `Batch Upload`_:

    .. code:: python

      entries = data_frame.to_dict(orient="records")

  - A gzipped NDJSON file for `File Upload`_ using the DataFrameHandler:

    .. code:: python

      from aito.utils.data_frame_handler import DataFrameHandler
      data_frame_handler = DataFrameHandler()
      data_frame_handler.df_to_format(
        df=data_frame,
        out_format='ndjson',
        write_output='path/to/myConvertedFile.ndjson.gz',
        convert_options={'compression': 'gzip'}
      )

.. _sdkQuickstartUploadData:

:ref:`Upload the Data <sdkUploadData>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :ref:`apiAitoClient` can upload the data with either `Batch Upload`_ or `File Upload`_:

.. code:: python

  from aito.utils.aito_client import AitoClient
  aito_client = AitoClient(instance_url="your_aito_instance_url", api_key="your_rw_api_key")

  # Batch upload
  aito_client.upload_entries(table_name='reddit', entries=entries)

  # File Upload

  with file_path.open(mode='rb') as in_f:
    aito_client.upload_binary_file(table_name='table_name', binary_file=in_f)

.. _Analyzer: https://aito.ai/docs/api/#schema-analyzer
.. _Batch Upload: https://aito.ai/docs/api/#post-api-v1-data-table-batch
.. _ColumnType: https://aito.ai/docs/api/#schema-column-type
.. _File Upload: https://aito.ai/docs/api/#post-api-v1-data-table-file
.. _Pandas DataFrame: https://pandas.pydata.org/pandas-docs/stable/reference/frame.html
.. _Python Dictionary Object: https://docs.python.org/3/tutorial/datastructures.html#dictionaries