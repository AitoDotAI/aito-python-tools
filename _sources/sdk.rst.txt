Aito SDK
==============

The Aito SDK consists of:

  - :py:mod:`~aito.schema`: Data structure for the Aito Database Schema
  - :py:mod:`~aito.v1`: The v1 API client to make requests to an Aito Database Instance
  - :py:mod:`~aito.v2`: A client for the Aito v2 API
  - :py:mod:`~aito.v1.requests`: Request objects used in AitoClient request so that you don't have to worry about the Aito API endpoint
  - :py:mod:`~aito.v1.responses`: Enriched response objects returned after executing a request with the AitoClient
  - :py:mod:`~aito.v1.api`: Different useful functions that uses an AitoClient object to interact with an Aito Database Instance
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

The :py:class:`~aito.v1.client.AitoClient` offers different functions to send a :py:mod:`Request object <aito.v1.requests>` to your Aito instance.

  - Make a request: :py:func:`~aito.v1.client.AitoClient.request`
  - Make a request asynchronously using `AIOHTTP ClientSession`_: :py:func:`~aito.v1.client.AitoClient.async_request`
  - Bounded asynchronous request with `asyncio semaphore`_: :py:func:`~aito.v1.client.AitoClient.bounded_async_request`
  - Make multiple requests asynchronously: :py:func:`~aito.v1.client.AitoClient.batch_requests`

.. _sdkVersions:

Choosing an API version
-----------------------

Each Aito API version has its own package, whose meaning never changes:

.. code:: python

    from aito.v1 import Client   # the v1 API: /api/v1
    from aito.v2 import Client   # the v2 API: /api/v2

``aito.Client`` is the one name that follows the current default version. It moves only on a
**major** release of ``aitoai``, so the major version tells you which API it is — 0.x is v1,
1.x will be v2 — and pinning ``aitoai<1`` keeps it on v1. Use ``aito.Client`` in quick
experiments; in production code import the explicit version.

Before 0.7 the v1 client lived in ``aito.client`` and the helpers in ``aito.api``, and the v2
client in ``aito.client.v2``. Those paths still work, resolve to the very same objects, and
emit a ``DeprecationWarning``; they are removed in 1.0. ``aito.client`` stays v1 until then —
it does **not** follow the default.

A bare ``pip install aitoai`` installs the API clients only. The command-line tool, schema
inference and file conversion need ``pip install 'aitoai[cli]'``.

.. _sdkAitoClientV2:

AitoClientV2
------------

The :py:class:`~aito.v2.client.AitoClientV2` talks to the **v2** API. It is a separate
class from the v1 :py:class:`~aito.v1.client.AitoClient` rather than a flag on it, because the two
APIs return genuinely different response shapes — the reasoning is written up in
``docs/v2-client-design.md``.

.. code:: python

    from aito.v2 import Client

    # the public read-only sandbox; use your own instance URL and key in production
    client = Client('https://shared.aito.ai/db/aito-demo',
                    'yg4rTlXkqDzm4y8gPeY75HCKaNwfbTQ2si64ONTi', env='v2')

    prediction = client.predict(
        from_table='invoices', where={'Description': 'cloud services'}, predict='GLCode')
    print(prediction.first.value, prediction.first.probability)   # E002 0.83...

A predict ranks **every** value of the field; the evidence in ``where`` changes each
candidate's probability but never removes one. To return only the values actually seen
with the evidence, filter on the per-candidate frequency ``$f`` with ``having``, via
:py:func:`~aito.v2.client.AitoClientV2.query`:

.. code:: python

    seen = client.query({
        'from': 'invoices',
        'where': {'Processor': 'Emily Davis'},
        'predict': 'GLCode',
        'select': ['$value', '$p', '$f'],
        'having': {'$f': {'$gte': 1}},
    })
    print([(hit.value, hit['$f']) for hit in seen])   # [('F001', 20)]

``having`` filters the ranked list after scoring: the remaining ``$p`` values are not
renormalised, and ``$f`` is the only field it accepts — ``{'$f': {'$gte' | '$gt' | '$lte' |
'$lt': <number>}}``. Filtering on anything else, such as ``$p``, is a ``400
request.invalid``.

Querying:

  - Predict a field's value: :py:func:`~aito.v2.client.AitoClientV2.predict`
  - Retrieve rows: :py:func:`~aito.v2.client.AitoClientV2.search`
  - Rank values by a goal: :py:func:`~aito.v2.client.AitoClientV2.recommend`
  - Find statistical relationships: :py:func:`~aito.v2.client.AitoClientV2.relate`
  - Match a link field's candidates against evidence: :py:func:`~aito.v2.client.AitoClientV2.match`
  - Estimate a numeric field: :py:func:`~aito.v2.client.AitoClientV2.estimate`
  - Aggregate: :py:func:`~aito.v2.client.AitoClientV2.aggregate`
  - Evaluate prediction quality: :py:func:`~aito.v2.client.AitoClientV2.evaluate`
  - Anything else, via the universal ``_query`` endpoint:
    :py:func:`~aito.v2.client.AitoClientV2.query`

.. note::

  The named methods post to v2's *enforced* named endpoints, which validate that the body
  matches the operation. A mismatch is a ``400`` naming the endpoint that wants that body,
  rather than a query that silently does something else.

Manipulating the database:

.. note::

  These operations require the client to be setup with the READ-WRITE API key

  - Create a collection: :py:func:`~aito.v2.client.AitoClientV2.create_collection`
  - Delete a collection: :py:func:`~aito.v2.client.AitoClientV2.delete_collection`
  - Upload batches of entries: :py:func:`~aito.v2.client.AitoClientV2.upload_entries`
  - Rebuild the index after a bulk load: :py:func:`~aito.v2.client.AitoClientV2.optimize`
  - Branch an environment: :py:func:`~aito.v2.client.AitoClientV2.branch_env`

Errors carry a machine-readable code, so you branch on the code rather than on the text of the
message:

.. code:: python

    from aito.v2 import Error

    try:
        client.delete_collection('invoices')
    except Error as err:
        if not err.is_not_found:   # a 404 here is the ordinary "drop if exists" case
            raise

Responses carry the engine's non-fatal warnings, which are the only in-band signal that the
server answered a slightly different query than the one you sent:

.. code:: python

    res = client.query({'from': 'invoices', 'where': {'no_such_column': 'x'}})
    for warning in res.warnings:
        print(warning.code, warning.message)

    # or make it a hard failure:
    strict = Client(instance_url, api_key, on_warning='raise')

Aito reports its own server-side processing time in the ``x-aitoai-response-time`` header,
which is what an application should surface rather than the round trip. The parsed body does
not carry it, so pass ``on_response``:

.. code:: python

    timings = []
    client = Client(instance_url, api_key,
                          on_response=lambda resp, path: timings.append(
                              (path, float(resp.headers['x-aitoai-response-time']))))

A complete runnable example — create a collection, load it, predict, explain, evaluate, drop it
— is in ``examples/v2_quickstart.py``.

.. _sdkAPI:

AitoAPI
-------
:py:mod:`aito.v1.api` module offers different functions that takes a :py:class:`Aito Client object <aito.v1.client.AitoClient>` as the first argument

  - Manipulate the database:

    .. note::

      These operations require the client to be setup with the READ-WRITE API key

    - Create a table: :py:func:`~aito.v1.api.create_table`
    - Delete a table: :py:func:`~aito.v1.api.delete_table`
    - Create the database: :py:func:`~aito.v1.api.create_database`
    - Delete the database: :py:func:`~aito.v1.api.delete_database`
    - Copy a table: :py:func:`~aito.v1.api.copy_table`
    - Rename a table: :py:func:`~aito.v1.api.rename_table`

  - Upload the data:

    .. note::

      These operations require the client to be setup with the READ-WRITE API key

    - Upload a binary file object to a table: :py:func:`~aito.v1.api.upload_binary_file`
    - Upload a file to a table: :py:func:`~aito.v1.api.upload_file`
    - Upload batches of entries to a table: :py:func:`~aito.v1.api.upload_entries`
    - Optimize a table after uploading the data: :py:func:`~aito.v1.api.optimize_table`


  - Get information about the database:

    - Get the instance version: :py:func:`~aito.v1.api.get_version`
    - Check if a table exists in the instance: :py:func:`~aito.v1.api.check_table_exists`
    - Get a list of existing tables in the instance: :py:func:`~aito.v1.api.get_existing_tables`
    - Get a table schema: :py:func:`~aito.v1.api.get_table_schema`
    - Find the number of entries in a table: :py:func:`~aito.v1.api.get_table_size`
    - Get the database schema: :py:func:`~aito.v1.api.get_database_schema`

  - Querying:

    - Query entries of a table: :py:func:`~aito.v1.api.query_entries`
    - Query all entries of a table: :py:func:`~aito.v1.api.query_all_entries`
    - Download a table: :py:func:`~aito.v1.api.download_table`

    - Make a job request (for query that takes longer than 30 seconds): :py:func:`~aito.v1.api.job_request`
    - Make a job request step by step: :py:func:`~aito.v1.api.create_job`, :py:func:`~aito.v1.api.get_job_status`, :py:func:`~aito.v1.api.get_job_result`

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
