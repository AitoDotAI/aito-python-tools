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

:ref:`Setup Aito credentials <cliSetUpAitoCredentials>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  The easiest way to set-up the credentials is by `configure` command::

    $ aito configure

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

  You can then upload the data by either:

    - :ref:`cliBatchUpload`::

        $ aito upload-entries tableName < tableEntries.json

    - :ref:`cliFileUpload`::

        $ aito upload-file tableName tableEntries.ndjson.gz

.. _sdkQuickstartUpload:

Upload Data with the SDK
------------------------

The Aito Python SDK uses `Pandas DataFrame`_ for multiple operations.

The example below shows how you can load a csv file into a DataFrame, please read the `official pandas guide <https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html>`__ for further instructions.
You can download an example csv file ``reddit_sample.csv`` `here <https://raw.githubusercontent.com/AitoDotAI/kickstart/master/reddit_sample.csv>`__ and run the code below:

  .. code-block:: python

    import pandas as pd

    reddit_df = pd.read_csv('reddit_sample.csv')

.. _sdkQuickstartInferTableSchema:

:ref:`Infer a Table Schema <sdkInferTableSchema>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can infer a :py:class:`~aito.schema.AitoTableSchema` from a `Pandas DataFrame`_:

  .. code-block:: python

    from aito.schema import AitoTableSchema
    reddit_schema = AitoTableSchema.infer_from_pandas_dataframe(reddit_schema)

.. _sdkQuickstartChangeSchema:

Change the Schema 
~~~~~~~~~~~~~~~~~

You might want to change the ColumnType_, e.g: The ``id`` column should be of type ``String`` instead of ``Int``,
or add a Analyzer_ to a ``Text`` column.

You can access and update the column schema by using the column name as the key:

  .. code-block:: python

    reddit_schema['label'].data_type = AitoStringType()
    # Change the analyzer of the `comments` column
    reddit_schema['comments'].analyzer = AitoTokenNgramAnalyzerSchema(
      source=AitoAliasAnalyzerSchema('en'),
      min_gram=1,
      max_gram=3
    )

.. _sdkQuickstartCreateTable:

:ref:`Create a Table <sdkCreateTable>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :py:class:`~aito.client.AitoClient` can create a table using a table name and a table schema:

  .. code-block:: python

    from aito.client import AitoClient
    aito_client = AitoClient(instance_url="your_aito_instance_url", api_key="your_rw_api_key")
    aito_client.create_table(table_name='reddit', table_schema=reddit_schema)

.. _sdkQuickstartConvertData:

Convert the Data
~~~~~~~~~~~~~~~~

The :py:class:`~aito.utils.data_frame_handler.DataFrameHandler` can convert a DataFrame to match an existing schema:

  .. code-block:: python

    from aito.utils.data_frame_handler import DataFrameHandler
    data_frame_handler = DataFrameHandler()
    converted_data_frame = data_frame_handler.convert_df_from_aito_table_schema(
      df=data_frame,
      table_schema=table_schema_content
    )

A DataFrame can be converted to:

  - A list of entries in JSON format for `Batch Upload`_:

    .. code-block:: python

      entries = data_frame.to_dict(orient="records")

  - A gzipped NDJSON file for `File Upload`_ using the DataFrameHandler:

    .. code-block:: python

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

The :py:class:`~aito.client.AitoClient` can upload the data with either `Batch Upload`_ or `File Upload`_:

  .. code-block:: python

    from aito.client import AitoClient
    aito_client = AitoClient(instance_url="your_aito_instance_url", api_key="your_rw_api_key")

    # Batch upload
    aito_client.upload_entries(table_name='reddit', entries=entries)

    # File Upload
    aito_client.upload_file(table_name='table_name', file_path=file_path)

The `Batch Upload`_ can also be done using a generator:

  .. code-block:: python

    def entries_generator(start, end):
      for idx in range(start, end):
        entry = {'id': idx}
        yield entry

    aito_client.upload_entries(
      table_name="table_name",
      entries=entries_generator(start=0, end=4),
      batch_size=2,
      optimize_on_finished=False
    )

.. _Analyzer: https://aito.ai/docs/api/#schema-analyzer
.. _Batch Upload: https://aito.ai/docs/api/#post-api-v1-data-table-batch
.. _ColumnType: https://aito.ai/docs/api/#schema-column-type
.. _File Upload: https://aito.ai/docs/api/#post-api-v1-data-table-file
.. _Pandas DataFrame: https://pandas.pydata.org/pandas-docs/stable/reference/frame.html
.. _Python Dictionary Object: https://docs.python.org/3/tutorial/datastructures.html#dictionaries
