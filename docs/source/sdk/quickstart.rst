SDK Quickstart
==============

Usage Examples
--------------

Uploading a data file to Aito
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. `Load a Data File to Pandas DataFrame`_
2. `Infer a Table Schema`_ (skip this step if you want to upload data to an existing table)
3. `Create Aito Schema`_ (skip this step if you want to upload data to an existing table)
4. `Upload Data`_

Load a Data File to Pandas DataFrame
------------------------------------

The Aito Python SDK uses `Pandas DataFrame`_ for multiple operations.

The example belows show how you can load a csv file into a DataFrame, please read the `official guide <https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html>`__ for further instructions.
You can download an example data file `here <https://raw.githubusercontent.com/AitoDotAI/kickstart/master/reddit_sample.csv>`__ and run the code below:

.. code:: python

  import pandas as pd

  reddit_df = pd.read_csv('reddit_sampe.csv')


Infer a Table Schema
--------------------

An Aito table schema describes how the table should be constructed and internally processed.
You can read more about the Aito schema `here <https://aito.ai/docs/articles/defining-a-database-schema/>`__

The Aito Python SDK includes a :ref:`apiSchemaHandler` that can infer an Aito table schema from a `Pandas DataFrame`_.
The example below assume that you already have a DataFrame named :ref:`reddit_df DataFrame<Load a Data File to Pandas DataFrame>`.

.. code:: python

  schema_handler = SchemaHandler()
  reddit_schema = schema_handler.infer_table_schema_from_pandas_data_frame(reddit_df)

  # Feels free to change the schema as you see fit. For example:
  # Change `label` type to `String` instead of `Int`
  reddit_schema['columns']['label']['type'] = 'String'
  # Use a different analyzer
  reddit_schema['columns']['comments']['analyzer'] = {
    "type": "token-ngram",
    "source": { "type": "language", "language": "english" },
    "minGram": 1,
    "maxGram": 3,
    "tokenSeparator": " "
  }


Create Aito Schema
------------------

You can create Aito schema with an :ref:`apiAitoClient`.

Your AitoClient must be set up with the READ-WRITE API key

.. code:: python

  from aito.utils.aito_client import AitoClient
  aito_client = AitoClient(instance_name="your_aito_instance_name", api_key="your_rw_api_key")

- `Create a table schema <https://aito.ai/docs/api/#put-api-v1-schema-table>`_

  .. code:: python

    # Aito table schema example
    table_schema = {
      'type': 'table',
      'columns': {
        'label': {'nullable': False, 'type': 'Int'},
        'comment': {'nullable': False, 'type': 'Text', 'analyzer': 'en'},
        'author': {'nullable': False, 'type': 'Text', 'analyzer': 'en'},
        'subreddit': {'nullable': False, 'type': 'String'},
        'score': {'nullable': False, 'type': 'Int'},
        'ups': {'nullable': False, 'type': 'Int'},
        'downs': {'nullable': False, 'type': 'Int'},
        'date': {'nullable': False, 'type': 'String'},
        'created_utc': {'nullable': False, 'type': 'Text'},
        'parent_comment': {'nullable': False, 'type': 'Text','analyzer': 'en'
        }
      }
    }

    aito_client.put_table_schema(table_name='reddit', table_schema=table_schema)

    # Check your table schema in Aito
    aito_client.get_table_schema(table_name=table_name)

- `Create a database schema <https://aito.ai/docs/api/#put-api-v1-schema>`_

  .. code:: python

    # Aito DB schema example
    database_schema = {
      'schema': {
        'reddit': {
          'type': 'table',
          'columns': {
            'label': {'nullable': False, 'type': 'Int'},
            'comment': {'nullable': False, 'type': 'Text', 'analyzer': 'en'},
            'author': {'nullable': False, 'type': 'Text', 'analyzer': 'en'},
            'subreddit': {'nullable': False, 'type': 'String'},
            'score': {'nullable': False, 'type': 'Int'},
            'ups': {'nullable': False, 'type': 'Int'},
            'downs': {'nullable': False, 'type': 'Int'},
            'date': {'nullable': False, 'type': 'String'},
            'created_utc': {'nullable': False, 'type': 'Text'},
            'parent_comment': {'nullable': False, 'type': 'Text','analyzer': 'en'
            }
          }
        }
      }
    }
    aito_client.put_database_schema(database_schema=database_schema)

    # Check your DB schema in Aito
    aito_client.get_database_schema()

Upload Data
-----------

You can create Aito schema with an :ref:`apiAitoClient`.

Your AitoClient must be set up with the READ-WRITE API key

.. code:: python

  from aito.utils.aito_client import AitoClient
  aito_client = AitoClient(instance_name="your_aito_instance_name", api_key="your_rw_api_key")

- `Upload a list of table entries <https://aito.ai/docs/api/#post-api-v1-data-table-batch>`__

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
    aito_client.populate_table_entries(table_name='reddit', entries=entries)

- Upload a `Pandas DataFrame`_

  .. code:: python

    # convert DataFrame to list of entries
    entries = df.to_dict(orient="records")
    aito_client.populate_table_entries(table_name='reddit', entries=entries)

- `Upload a gzipped ndjson file <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__

  .. code:: python

    with file_path.open(mode='rb') as in_f:
      aito_client.populate_table_by_file_upload(table_name='table_name', binary_file_object=in_f)

Delete data
-----------

You can delete the data with an :ref:`apiAitoClient`.

Your AitoClient must be set up with the READ-WRITE API key

- Delete a table: :meth:`aito.utils.aito_client.AitoClient.delete_table`
- Delete the entire database :meth:`aito.utils.aito_client.AitoClient.delete_table`

.. _Pandas DataFrame: https://pandas.pydata.org/pandas-docs/stable/reference/frame.html