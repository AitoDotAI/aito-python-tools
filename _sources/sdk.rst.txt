Aito SDK
==============

The Aito SDK consists of:

  - :py:mod:`~aito.schema`: Data structure for the Aito Database Schema
  - :py:mod:`~aito.client`: A versatile client to make requests to an Aito Database Instance
  - :py:mod:`~aito.client.v2`: A client for the Aito v2 API
  - :py:mod:`~aito.client_request`: Request objects used in AitoClient request so that you don't have to worry about the Aito API endpoint
  - :py:mod:`~aito.client_response`: Enriched response objects returned after executing a request with the AitoClient
  - :py:mod:`~aito.api`: Different useful functions that uses an AitoClient object to interact with an Aito Database Instance
  - :py:class:`~aito.utils.data_frame_handler.DataFrameHandler`: Utility to read, write, and convert a Pandas DataFrame in accordance to a Aito Table Schema

.. note::

  We highly recommend you to take a look at the :ref:`quickstart guide to uploading data <sdkQuickstartUpload>` if you haven't already.  

.. _sdkAitoSchema:

AitoSchema
----------

Before uploading data into Aito, you need to create a table with a :py:class:`~aito.schema.AitoTableSchema`.

You can infer a table schema from a `Pandas DataFrame`_  with :py:func:`~aito.schema.AitoTableSchema.infer_from_pandas_data_frame`.

You can also create a table schema column-by-column and infer the :py:class:`~aito.schema.AitoColumnTypeSchema` with :py:func:`~aito.schema.AitoColumnTypeSchema.infer_from_samples`.

.. _sdkAitoClient:

AitoClient
----------

The :py:class:`~aito.client.AitoClient` offers different functions to send a :py:mod:`Request object <aito.client_request>` to your Aito instance.

  - Make a request: :py:func:`~aito.client.AitoClient.request`
  - Make a request asynchronously using `AIOHTTP ClientSession`_: :py:func:`~aito.client.AitoClient.async_request`
  - Bounded asynchronous request with `asyncio semaphore`_: :py:func:`~aito.client.AitoClient.bounded_async_request`
  - Make multiple requests asynchronously: :py:func:`~aito.client.AitoClient.batch_requests`

.. _sdkAitoClientV2:

AitoClientV2
------------

The :py:class:`~aito.client.v2.client.AitoClientV2` talks to the **v2** API. It is a separate
class from the v1 :py:class:`~aito.client.AitoClient` rather than a flag on it, because the two
APIs return genuinely different response shapes — the reasoning is written up in
``docs/v2-client-design.md``.

.. code:: python

    from aito.client.v2 import AitoClientV2

    client = AitoClientV2(instance_url, api_key)

    prediction = client.predict(
        from_table='invoices', where={'vendor': 'Elenia Oy'}, predict='gl_code')
    print(prediction.first.value, prediction.first.probability)

Querying:

  - Predict a field's value: :py:func:`~aito.client.v2.client.AitoClientV2.predict`
  - Retrieve rows: :py:func:`~aito.client.v2.client.AitoClientV2.search`
  - Rank values by a goal: :py:func:`~aito.client.v2.client.AitoClientV2.recommend`
  - Find statistical relationships: :py:func:`~aito.client.v2.client.AitoClientV2.relate`
  - Estimate a numeric field: :py:func:`~aito.client.v2.client.AitoClientV2.estimate`
  - Aggregate: :py:func:`~aito.client.v2.client.AitoClientV2.aggregate`
  - Evaluate prediction quality: :py:func:`~aito.client.v2.client.AitoClientV2.evaluate`
  - Anything else, via the universal ``_query`` endpoint:
    :py:func:`~aito.client.v2.client.AitoClientV2.query`

.. note::

  The named methods post to v2's *enforced* named endpoints, which validate that the body
  matches the operation. A mismatch is a ``400`` naming the endpoint that wants that body,
  rather than a query that silently does something else.

Manipulating the database:

.. note::

  These operations require the client to be setup with the READ-WRITE API key

  - Create a collection: :py:func:`~aito.client.v2.client.AitoClientV2.create_collection`
  - Delete a collection: :py:func:`~aito.client.v2.client.AitoClientV2.delete_collection`
  - Upload batches of entries: :py:func:`~aito.client.v2.client.AitoClientV2.upload_entries`
  - Rebuild the index after a bulk load: :py:func:`~aito.client.v2.client.AitoClientV2.optimize`
  - Branch an environment: :py:func:`~aito.client.v2.client.AitoClientV2.branch_env`

Errors carry a machine-readable code, so you branch on the code rather than on the text of the
message:

.. code:: python

    from aito.client.v2 import AitoV2Error

    try:
        client.delete_collection('invoices')
    except AitoV2Error as err:
        if not err.is_not_found:   # a 404 here is the ordinary "drop if exists" case
            raise

Responses carry the engine's non-fatal warnings, which are the only in-band signal that the
server answered a slightly different query than the one you sent:

.. code:: python

    res = client.query({'from': 'invoices', 'where': {'no_such_column': 'x'}})
    for warning in res.warnings:
        print(warning.code, warning.message)

    # or make it a hard failure:
    strict = AitoClientV2(instance_url, api_key, on_warning='raise')

A complete runnable example — create a collection, load it, predict, explain, evaluate, drop it
— is in ``examples/v2_quickstart.py``.

.. _sdkAPI:

AitoAPI
-------
:py:mod:`aito.api` module offers different functions that takes a :py:class:`Aito Client object <aito.client.AitoClient>` as the first argument

  - Manipulate the database:

    .. note::

      These operations require the client to be setup with the READ-WRITE API key

    - Create a table: :py:func:`~aito.api.create_table`
    - Delete a table: :py:func:`~aito.api.delete_table`
    - Create the database: :py:func:`~aito.api.create_database`
    - Delete the database: :py:func:`~aito.api.delete_database`
    - Copy a table: :py:func:`~aito.api.copy_table`
    - Rename a table: :py:func:`~aito.api.rename_table`

  - Upload the data:

    .. note::

      These operations require the client to be setup with the READ-WRITE API key

    - Upload a binary file object to a table: :py:func:`~aito.api.upload_binary_file`
    - Upload a file to a table: :py:func:`~aito.api.upload_file`
    - Upload batches of entries to a table: :py:func:`~aito.api.upload_entries`
    - Optimize a table after uploading the data: :py:func:`~aito.api.optimize_table`


  - Get information about the database:

    - Get the instance version: :py:func:`~aito.api.get_version`
    - Check if a table exists in the instance: :py:func:`~aito.api.check_table_exists`
    - Get a list of existing tables in the instance: :py:func:`~aito.api.get_existing_tables`
    - Get a table schema: :py:func:`~aito.api.get_table_schema`
    - Find the number of entries in a table: :py:func:`~aito.api.get_table_size`
    - Get the database schema: :py:func:`~aito.api.get_database_schema`

  - Querying:

    - Query entries of a table: :py:func:`~aito.api.query_entries`
    - Query all entries of a table: :py:func:`~aito.api.query_all_entries`
    - Download a table: :py:func:`~aito.api.download_table`

    - Make a job request (for query that takes longer than 30 seconds): :py:func:`~aito.api.job_request`
    - Make a job request step by step: :py:func:`~aito.api.create_job`, :py:func:`~aito.api.get_job_status`, :py:func:`~aito.api.get_job_result`

.. _sdkTroubleshooting:

Troubleshooting
---------------

The easiest way to troubleshoot the Aito SDK is by enabling the debug logging. You can enable the debug logging by:

.. testcode::

    import logging

    logging.basicConfig(level=logging.DEBUG)


.. _Pandas DataFrame: https://pandas.pydata.org/pandas-docs/stable/reference/frame.html
.. _AIOHTTP ClientSession: https://docs.aiohttp.org/en/stable/client.html
.. _asyncio semaphore: https://docs.python.org/3/library/asyncio-sync.html#asyncio.Semaphore
