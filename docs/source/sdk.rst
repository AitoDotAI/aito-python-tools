Aito SDK
==============

The Aito SDK consists of:
  - :py:mod:`~aito.schema`: Data structure for the Aito Database Schema
  - :py:mod:`~aito.client`: A versatile client that helps you to interact with the Aito Database Instance
  - :py:class:`~aito.utils.data_frame_handler.DataFrameHandler`: Utility to read, write, and convert a Pandas DataFrame in accordance to a Aito Table Schema

We highly recommend you to take a look at the :ref:`quickstart guide to upload data <sdkQuickstartUpload>` if you haven't


.. _sdkAitoSchema:

AitoSchema
----------

Before uploading data into Aito, you need to create a table with a :py:class:`~aito.schema.AitoTableSchema`.

You can infer a table schema from a `Pandas DataFrame`_  with :py:func:`~aito.schema.AitoTableSchema.infer_from_pandas_data_frame`.

You can also create a table schema column-by-column and infer the :py:class:`~aito.schema.AitoColumnTypeSchema` with :py:func:`~aito.schema.AitoColumnTypeSchema.infer_from_samples`.

.. _sdkAitoClient:

AitoClient
----------

The :py:class:`~aito.client.AitoClient` offers multiple functions:

  - Manipulate the database:

    .. note::

      These operations require the client to be setup with the READ-WRITE API key

    - Create a table: :py:func:`~aito.client.AitoClient.create_table`
    - Delete a table: :py:func:`~aito.client.AitoClient.delete_table`
    - Create the database: :py:func:`~aito.client.AitoClient.create_database`
    - Delete the database: :py:func:`~aito.client.AitoClient.delete_database`
    - Copy a table: :py:func:`~aito.client.AitoClient.copy_table`
    - Rename a table: :py:func:`~aito.client.AitoClient.rename_table`

  - Upload the data:

    .. note::

      These operations require the client to be setup with the READ-WRITE API key

    - Upload a binary file object to a table: :py:func:`~aito.client.AitoClient.upload_binary_file`
    - Upload a file to a table: :py:func:`~aito.client.AitoClient.upload_file`
    - Upload batches of entries to a table: :py:func:`~aito.client.AitoClient.upload_entries`
    - Optimize a table after uploading the data: :py:func:`~aito.client.AitoClient.optimize_table`


  - Get information about the database:

    - Get the instance version: :py:func:`~aito.client.AitoClient.get_version`
    - Check if a table exists in the instance: :py:func:`~aito.client.AitoClient.check_table_exists`
    - Get a list of existing tables in the instance: :py:func:`~aito.client.AitoClient.get_existing_tables`
    - Get a table schema: :py:func:`~aito.client.AitoClient.get_table_schema`
    - Find the number of entries in a table: :py:func:`~aito.client.AitoClient.get_table_size`
    - Get the database schema: :py:func:`~aito.client.AitoClient.get_database_schema`

  - Querying:

    - Query entries of a table: :py:func:`~aito.client.AitoClient.query_entries`
    - Query all entries of a table: :py:func:`~aito.client.AitoClient.query_all_entries`
    - Download a table: :py:func:`~aito.client.AitoClient.download_table`
    - Make a request to an Aito API endpoint: :py:func:`~aito.client.AitoClient.request`
    - Make multiple requests asynchronously: :py:func:`~aito.client.AitoClient.async_requests`
    - Make a job request (for query that takes longer than 30 seconds): :py:func:`~aito.client.AitoClient.job_request`
    - Make a job request step by step: :py:func:`~aito.client.AitoClient.create_job`, :py:func:`~aito.client.AitoClient.get_job_status`, :py:func:`~aito.client.AitoClient.get_job_result`

.. _sdkTroubleshooting:

Troubleshooting
---------------

The easiest way to troubleshoot the Aito SDK is by enabling the debug logging. You can enable the debug logging by:

.. testcode::

    import logging

    logging.basicConfig(level=logging.DEBUG)


.. _Pandas DataFrame: https://pandas.pydata.org/pandas-docs/stable/reference/frame.html
