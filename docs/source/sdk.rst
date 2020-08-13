Aito SDK
==============

:ref:`Quickstart guide to upload data <sdkQuickstartUpload>`


.. _sdkLoadDataFile:

Load a Data File to Pandas DataFrame
------------------------------------

The Aito Python SDK uses `Pandas DataFrame`_ for multiple operations.

The example below shows how you can load a csv file into a DataFrame, please read the `official pandas guide <https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html>`__ for further instructions.
You can download an example csv file ``reddit_sample.csv`` `here <https://raw.githubusercontent.com/AitoDotAI/kickstart/master/reddit_sample.csv>`__ and run the code below:

.. code:: python

  import pandas as pd

  reddit_df = pd.read_csv('reddit_sample.csv')

.. _sdkInferTableSchema:

Infer a Table Schema
--------------------

An Aito table schema describes how the table should be constructed and processed internally.
You can read more about the Aito schema `here <https://aito.ai/docs/articles/defining-a-database-schema/>`__

The :py:class:`~aito.schema.AitoTableSchema` can be inferred from a `Pandas DataFrame`_.
The example below assumes that you already have a DataFrame named reddit_df from :ref:`sdkLoadDataFile`

.. code:: python

  from aito.schema import AitoTableSchema, AitoStringType, AitoTokenNgramAnalyzerSchema, AitoAliasAnalyzerSchema
  reddit_schema = AitoTableSchema.infer_from_pandas_data_frame(reddit_df)

  # Feel free to change the schema as you see fit. For example:
  # Change the data type  of the `label` column  to `String` instead of `Int`
  reddit_schema['label'].data_type = AitoStringType()
  # Change the analyzer of the `comments` column
  reddit_schema['comments'].analyzer = AitoTokenNgramAnalyzerSchema(
    source=AitoAliasAnalyzerSchema('en'),
    min_gram=1,
    max_gram=3
  )

.. _sdkCreateTable:

Create a Table
--------------

You can `create a table <https://aito.ai/docs/api/#put-api-v1-schema-table>`__ after you have the table schema with the :py:class:`~aito.client.AitoClient`.

Your AitoClient must be set up with the READ-WRITE API key

The example below assumes that you already have a table_schema named reddit_schema from :ref:`sdkInferTableSchema`.

.. code:: python

  from aito.client import AitoClient
  aito_client = AitoClient(instance_url="your_aito_instance_url", api_key="your_rw_api_key")
  aito_client.create_table(table_name='reddit', table_schema=reddit_schema)

  # Check your table schema in Aito
  aito_client.get_table_schema(table_name=table_name)

- `To create a database schema <https://aito.ai/docs/api/#put-api-v1-schema>`_:

  .. code:: python

    # Aito DB schema example
    from aito.schema import AitoDatabaseSchema
    database_schema = AitoDatabaseSchema(tables={'reddit': reddit_schema})
    aito_client.create_database(database_schema=database_schema)

    # Check your DB schema in Aito
    aito_client.get_database_schema()

.. _sdkUploadData:

Upload Data
-----------

You can upload data to a table with the :py:class:`~aito.client.AitoClient`.

Your AitoClient must be set up with the READ-WRITE API key

.. code:: python

  from aito.client import AitoClient
  aito_client = AitoClient(instance_url="your_aito_instance_url", api_key="your_rw_api_key")

- `Upload a list of table entries <https://aito.ai/docs/api/#post-api-v1-data-table-batch>`__ with :py:func:`~aito.client.AitoClient.upload_entries`

  .. code:: python

    entries = [
      {
        'label': 0,
        'comment': 'it was.',
        'author': 'renden123',
        'subreddit': 'CFB',
        'score': 4,
        'ups': -1,
        'downs': -1,
        'date': '2016-11',
        'created_utc': '2016-11-22 21:32:03',
        'parent_comment': "Wasn't it 2010?"
      }
    ]
    aito_client.upload_entries(table_name='reddit', entries=entries)

- Upload a `Pandas DataFrame`_

  .. code:: python

    # convert DataFrame to list of entries
    entries = df.to_dict(orient="records")
    aito_client.upload_entries(table_name='reddit', entries=entries)

- `Upload a gzipped ndjson file <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__ with :py:func:`~aito.client.AitoClient.upload_file`

  .. code:: python

    aito_client.upload_file(table_name='table_name', file_path=file_path)

- Upload using generator

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

Delete data
-----------

You can delete data with the :py:class:`~aito.client.AitoClient`.

Your AitoClient must be set up with the READ-WRITE API key

- Delete a table: :py:func:`aito.client.AitoClient.delete_table`
- Delete the entire database :py:func:`aito.client.AitoClient.delete_database`

.. _Pandas DataFrame: https://pandas.pydata.org/pandas-docs/stable/reference/frame.html


.. _sdkExecuteQuery:

Execute Queries
---------------

You can execute queries with the :py:class:`~aito.client.AitoClient`.

Your AitoClient can be set up with the READ-ONLY API key

:meth:`Request to an endpoint <aito.client.AitoClient.request>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The example below show how you could send a predict query to Aito:

.. code:: python

  aito_client.request(
    method='POST',
    endpoint='/api/v1/_predict',
    query={
      'from': 'invoice',
      'where': {
        'description': 'a very long invoice description'
      },
      'predict': 'sales_rep'
    }
  )

:meth:`Query a Table Entries <aito.client.AitoClient.query_entries>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

  # query the first 10 entries of a table
  aito_client.query_entries(table_name='table_name')


:meth:`Executing multiple queries asynchronously <aito.client.AitoClient.async_requests>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

  # predict with different descriptions

  descriptions = ['first description', 'second description', 'third description']

  responses = aito_client.async_requests(
    methods=['POST'] * len(descriptions),
    endpoints=['/api/v1/_predict'] * len(descriptions),
    queries=[
      {
        'from': 'invoice',
        'where': {
          'description': desc
        },
        'predict': 'sales_rep'
      }
      for desc in descriptions
    ]
  )

:meth:`Sending a job request for query that takes longer than 30 seconds <aito.client.AitoClient.job_request>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some queries might take longer than 30 seconds to run (e.g: `Evaluate <https://aito.ai/docs/api/#post-api-v1-evaluate>`_).
You can use the job request for these queries. For example:

.. code:: python

  response = aito_client.job_request(
    job_endpoint='/api/v1/jobs/_evaluate',
    query={
      "test": {
        "$index": {
          "$mod": [4, 0]
        }
      },
      "evaluate": {
        "from": "invoice",
        "where": {
          "description": { "$get": "description" }
        },
        "predict": "sales_rep"
      }
    }
  )
