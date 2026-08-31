Changelog
=========

0.6.2
-----

Importing the v2 client no longer drags in the v1 stack's heavy dependencies.

SDK
^^^

Fixed
"""""

- ``from aito.client.v2 import AitoClientV2`` no longer imports **pandas**, **numpy**,
  **aiohttp** or **langdetect**. The v2 client is a plain HTTP client whose only
  third-party dependency is ``requests``, but importing a submodule runs the parent
  package's ``__init__``, and that chain reached :py:mod:`aito.schema`, which imported
  pandas at module scope. Beyond the weight, it meant a service that had never touched a
  dataframe **failed to start** wherever numpy's compiled extensions could not load.
  Found by the first application to adopt the v2 client
- ``pandas`` and ``langdetect`` are now imported inside the functions in
  :py:mod:`aito.schema` that use them, and ``aiohttp`` inside the asynchronous paths of
  :py:class:`~aito.client.AitoClient`. Every feature behaves as before; only the moment
  of import changed. The v1 client and the CLI import faster as a side effect
- Import cost of ``import aito.client.v2``: 933 modules before, 409 after

.. note::

  ``pip install aitoai`` still installs pandas and the other file-format and CLI
  dependencies -- they remain in ``install_requires``. Moving them behind extras
  (``aitoai[cli]``) is a breaking packaging change and is tracked separately.

0.6.1
-----

Follow-ups from the first real consumer of the v2 client, and from Aito engine v2.7.0.

SDK
^^^

Added
"""""

- :py:func:`~aito.client.v2.client.AitoClientV2.match` — rank the candidate values of a
  link field against some evidence, the operator behind record matching (a bank payment
  against the invoice it settles). Deliberately absent in 0.6.0 because ``_match`` could
  not rank on v2; engine v2.7.0 fixed that, so the reason is gone. Hits carry v1's
  ``feature`` and ``field`` keys alongside ``$p`` and ``$value`` — v2 keeps a v1 key and
  adds the v2 one rather than replacing it
- ``on_response`` — an optional callback invoked with the raw ``requests.Response`` after
  every call, successful or not. The only way to reach what the parsed body does not
  carry: notably ``x-aitoai-response-time``, Aito's own server-side processing time, which
  is what an application should show rather than the round trip. v2 did not send that
  header at all before engine v2.7.0

Engine v2.7.0 behaviour
"""""""""""""""""""""""

- ``_estimate`` / ``_aggregate`` / ``_evaluate`` now return the same envelope whichever
  storage engine answers. The client's tolerance for the older bare shape is kept, so one
  client works against both an older engine and a current one
- ``_estimate`` on a legacy table carries both ``data.value`` and ``data.estimate``.
  :py:attr:`~aito.client.v2.responses.V2EstimateResponse.value` prefers ``value``
- ``_batch`` now returns the documented ``{kind, data}`` envelope instead of a bare array,
  with each element tagged ``kind: "rows"``. Both forms are still accepted
- Errors from the schema and data endpoints are now the structured ``error`` kind, so
  :py:attr:`~aito.client.v2.errors.AitoV2Error.code` is populated on them. The
  status-code fallback in :py:attr:`~aito.client.v2.errors.AitoV2Error.is_not_found`
  is kept for older engines

0.6.0
-----

This version adds a client for the Aito **v2** API.

SDK
^^^

Aito v2 client
""""""""""""""

- Added :py:class:`~aito.client.v2.client.AitoClientV2`, a client for the v2 API, in the new
  :py:mod:`aito.client.v2` package. The v1 :py:class:`~aito.client.AitoClient` is unchanged;
  v2 is a separate class rather than a flag, because the response shapes genuinely differ.
  The reasoning is written up in ``docs/v2-client-design.md``
- Query methods address the **enforced named endpoints** (``_predict``, ``_search``,
  ``_recommend``, ``_relate``), so posting a body whose mode does not match the operation is a
  ``400`` naming the right endpoint rather than a silently different query.
  :py:func:`~aito.client.v2.client.AitoClientV2.query` posts to the universal ``_query``
- Added typed responses: :py:class:`~aito.client.v2.responses.V2RowsResponse`,
  :py:class:`~aito.client.v2.responses.V2EstimateResponse`,
  :py:class:`~aito.client.v2.responses.V2AggregateResponse`,
  :py:class:`~aito.client.v2.responses.V2EvaluationResponse` and
  :py:class:`~aito.client.v2.responses.V2BatchResponse`
- Predicted values are read as ``$value``, v2's own name for them. They are deliberately not
  aliased back to v1's ``feature``
- Schema, data and environment operations: create and delete collections, batch upload,
  ``optimize``, ``_modify``, ``_delete``, and branching or deleting an environment

Errors and warnings
"""""""""""""""""""

- :py:class:`~aito.client.v2.errors.AitoV2Error` exposes v2's machine-readable error
  ``code`` (``not_found``, ``request.invalid``, ``query.invalid``, ...) alongside the HTTP
  status, so a caller branches on the code instead of matching text in the message.
  :py:attr:`~aito.client.v2.errors.AitoV2Error.is_not_found` covers the drop-if-exists idiom
- The v2 ``warnings`` channel is surfaced on every response as
  :py:attr:`~aito.client.v2.responses.V2Response.warnings`. The client's ``on_warning``
  argument (``'ignore'``, ``'log'``, ``'raise'``) decides what happens to them — ``'raise'``
  turns a query the server had to broaden into a hard failure
- Environment names that the engine reserves (``_``, ``env.``, ``release.`` prefixes) are
  rejected locally with a message naming the fix

Known API behaviour handled by the client
"""""""""""""""""""""""""""""""""""""""""

- ``_estimate``, ``_aggregate`` and ``_evaluate`` return the ``{kind, data}`` envelope for a
  v2 collection but the flat v1 shape for a legacy table served by the compatibility shim.
  The response classes read both, so ``.value`` and ``.accuracy`` work either way
- ``relate`` takes a list of field names on v2 where v1 took a bare string. A single name
  given as a string is wrapped for you

Examples
""""""""

- Added ``examples/v2_quickstart.py``, a runnable end-to-end script that creates a
  collection, loads it, predicts, explains, evaluates and drops it again

0.5.4
-----

This version adds support for new Aito schema types and improves schema inference accuracy.

SDK
^^^

Schema Type Support
"""""""""""""""""""

- Added support for **Json** type (:py:class:`~aito.schema.AitoJsonType`) for columns containing dictionaries or complex nested structures
- Added support for **Array types** (``Boolean[]``, ``Int[]``, ``Decimal[]``, ``String[]``) with automatic element type inference
- Array columns are automatically detected when all non-null values are lists

Improved Schema Inference
"""""""""""""""""""""""""

- **Natural language detection**: Text fields containing natural language (sentences with punctuation, common words) now correctly use language analyzers instead of delimiter analyzers, even when delimiters like commas are present
- **Integer preference**: Floating-point values that are whole numbers (e.g., ``1.0``, ``2.0``) are now inferred as ``Int`` instead of ``Decimal``
- **Date pattern detection**: Date strings in common formats (ISO, US, EU) are now inferred as ``String`` type without analyzer, enabling exact matching

0.5.3
-----

This version updates Python version support and modernizes the project infrastructure.

General
^^^^^^^

- Dropped support for Python 3.6, 3.7, and 3.8 (all end-of-life). Minimum supported version is now Python 3.9.
- Updated CI/CD to test only on Python 3.11.
- Migrated documentation hosting from ReadTheDocs to GitHub Pages.
- Added ``./do deploy-docs`` command for manual documentation deployment.

0.5.2
-----

This version adds support for additional Aito API endpoints and helper classes for data modifications.

SDK
^^^

- Added :py:func:`~aito.api.estimate` function for the `Estimate API <https://aito.ai/docs/api/#post-api-v1-estimate>`__
- Added :py:func:`~aito.api.aggregate` function for the `Aggregate API <https://aito.ai/docs/api/#post-api-v1-aggregate>`__
- Added :py:func:`~aito.api.modify` function for the `Modify API <https://aito.ai/docs/api/#post-api-v1-data-modify>`__
- Added :py:func:`~aito.api.batch` function for the `Batch API <https://aito.ai/docs/api/#post-api-v1-batch>`__
- Added helper classes for modify operations: :py:class:`~aito.client.requests.Insert`, :py:class:`~aito.client.requests.Update`, :py:class:`~aito.client.requests.Delete`
- Added corresponding request and response classes: ``EstimateRequest``, ``EstimateResponse``, ``AggregateRequest``, ``AggregateResponse``, ``ModifyRequest``, ``ModifyResponse``, ``BatchRequest``, ``BatchResponse``

0.5.1
-----

This version adds support for multitenant Aito instances and array datatypes.

SDK
^^^

- Added support for multitenant instance URLs (e.g., ``https://shared.aito.ai/db/my-database``)
- Added ``is_multitenant`` property to :py:class:`~aito.client.AitoClient` to detect multitenant instances
- File upload operations now automatically use streaming batch uploads for multitenant instances,
  as S3 file upload is not available in multitenant environments
- Added support for array datatypes: ``Boolean[]``, ``Int[]``, ``Decimal[]``, ``String[]``, ``Text[]``

0.5.0
-----

Parquet file format support added.

Library dependencies updated to more modern versions. E.g. Pandas 2 is now supported.

0.4.3
-----
 This version adds better support for performing jobs on Aito.

 - There's a new command `optimize-table` (`<https://aito.ai/docs/api/#post-api-v1-data-table-optimize>`), which can
    be used to improve query times at the expense of slower writes.
 - Optimize, described above, now runs as a job by default (`<https://aito.ai/docs/api/#post-api-v1-jobs-query>`_). This is
   to avoid timeouts during the operation on non-trivial tables.

0.4.2
-----
 This version fixes a validation discrepancy between Aito Core and the SDK. Previously the
 SDK has allowed creating tables with names containing whitespaces. This is a validation bug,
 since the validation was performed in a slightly different way, depending on the context.

 Now the SDK does no longer allow whitespaces in the names and gives a validation error if
 the user tries to create a table with an disallowed name.

0.4.1
-----

In this version the data conversions and especially CSV reading have been made more robust
and lenient.

Errors regarding large integers have been managed by converting integer fields
with out-of-bounds values into string fields. Issues caused by extra commas in CSV are now
treated by ignoring the additional commas and issuing a warning in the log.

Schema inference has been improved by inferring String type for the empty columns instead of
Decimal type. Schema inference no more fails in situations, where the column contains only
whitespaces.

SDK
^^^

aito.schema package now contains DataSeriesProperties, which is used to infer a column's
Aito datatype. DataSeriesProperties now keeps track of the smallest and the largest
value in the series in order to infer a type according to the number bounds:

  - :py:class:`~aito.schema.DataSeriesProperties`

0.4.0
-----

This version contains changes on how you make requests with the :py:class:`~aito.client.AitoClient`.
In addition to specifying the request method and endpoint as before, you can now use different
:py:mod:`Request objects <aito.client.requests>` or use the endpoint methods in the API,
i.e: :py:func:`aito.api.predict`.
The client now returns enriched :py:mod:`Response objects <aito.client.responses>` instead of JSON as before.

Helper methods of the AitoClient, e.g: create_table, are moved to the :py:mod:`aito.api` module.

You can now execute requests from the CLI.


SDK
^^^

- Schema objects validation in the :py:mod:`aito.schema` module is improved.
- Refactored the client into the **aito.client** subpacakge:
  - Added the :py:mod:`aito.client.requests` subpackage which contains Request Classes for the **AitoClient**.
  - Added the :py:mod:`aito.client.responses` subpackage which contains Response Classes returned by the **AitoClient**.
  - You can import the **AitoClient** the same way as before:

    .. code-block:: python

      from aito.client import AitoClient

    You can import the request and response classes directly from the subpackage:

    .. code-block:: python

      from aito.client import PredictRequest, PredictResponse

AitoClient
""""""""""

  - The **AitoClient.request** method no longer takes positional arguments. You now have to specify either `request_obj` or `method` and `endpoint`.
  - Both **AitoClient** and **AitoClient.request** now have a `raise_for_status` argument which controls whether the client should raise or return an **aito_client.RequestError** object when an error occurs during sending a request.
  - Added the **aito.AitoClient.async_request** method to execute a request asynchronously using `aiohttp ClientSession`_
  - The **async_requests** method is deprecated, use **AitoClient.batch_requests** instead.


API functions
"""""""""""""
- Helper methods of the AitoClient are moved to the :py:mod:`aito.api` module. The functions in the api module takes an **AitoClient** object as the first argument

  .. code-block:: python

    from aito.client import AitoClient
    from aito.api import get_database_schema

    client = AitoClient(your_instance_url, your_instance_api_key)
    get_database_schema(client)

- Added the endpoint methods to send a query to Aito API Endpoint: **search**, **predict**, **recommend**, **evaluate**, **similarity**, **match**, **relate**, **generic_query**
- Added the following new api functions: **create_column**, **get_column_schema**, **delete_column**,  **delete_entries**, **quick_add_table**,  **quick_predict (BETA)** and **quick_predict_and_evaluate (BETA)**

CLI
^^^
- Added the following commands to send a query to Aito API Endpoint: **search**, **predict**, **recommend**, **evaluate**, **similarity**, **match**, **relate**, **generic-query**
- Added the **create-database** command to create database using the Database Schema
- Removed the **--encoding** flag in the **convert** and the **infer-table-schema** command
- **Beta**: Added the **quick-predict** command to generate an example predict query and evaluate its performance

0.3.1
-----

- Improved and fixed error codes in documentation
- Added the :py:func:`aito.schema.AitoColumnTypeSchema.infer_from_samples` function that infers the Column Type from samples.

0.3.0
-----

SDK
^^^

Refactoring
"""""""""""
- | The :py:mod:`aito.client` module is moved from the **sdk** subpackage to the main **aito** package.
  | You can now import the :py:class:`~aito.client.AitoClient` by:

  .. code-block:: python

    from aito.client import AitoClient
    # previously: from aito.sdk.aito_client import AitoClient

- | The :py:mod:`~aito.utils.data_frame_handler` and :py:mod:`~aito.utils.sql_connection` module is moved from the **sdk** subpackage to the **utils** subpackage.
  | You can now import the :py:class:`~aito.utils.data_frame_handler.DataFrameHandler` and :py:class:`~aito.utils.sql_connection.SQLConnection` by:

  .. code-block:: python

    from aito.utils.data_frame_handler import DataFrameHandler
    # previously: from aito.sdk.data_frame_handler import DataFrameHandler
    from aito.utils.sql_connection import SQLConnection
    # previously: from aito.sdk.sql_connection import SQLConnection

New features
""""""""""""

- Added the :py:mod:`aito.schema` module which contains the component object of the Aito Schema including:

  - :py:class:`~aito.schema.AitoAnalyzerSchema`
  - :py:class:`~aito.schema.AitoDataTypeSchema`
  - :py:class:`~aito.schema.AitoColumnTypeSchema`
  - :py:class:`~aito.schema.AitoTableSchema`
  - :py:class:`~aito.schema.AitoDatabaseSchema`

  Please go to the :py:mod:`module page <aito.schema>` for a full list of the supported components

- Minor changes:

  - Improved `Analyzer`_ inference that can now detect `Delimiter Analyzer`_ and is exposed at :py:func:`aito.schema.AitoAnalyzerSchema.infer_from_samples`
  - :py:func:`aito.client.AitoClient.get_table_schema` and :py:func:`aito.client.AitoClient.get_database_schema` now return the schema object instead of the JSON response
  - :py:func:`aito.client.AitoClient.create_table`, :py:func:`aito.utils.data_frame_handler.DataFrameHandler.convert_df_using_aito_table_schema` and :py:func:`aito.utils.data_frame_handler.DataFrameHandler.convert_file` now also support input of AitoTableSchema object
  - :py:func:`aito.client.AitoClient.query_entries` now returns a list of table entries instead of the JSON response
  - :py:func:`aito.client.AitoClient.query_entries` and :py:func:`aito.client.AitoClient.query_all_entries` now supports the ``select`` keyword to select the fields of an entry

Deprecation
"""""""""""

- The **SchemaHandler** is deprecated and will be removed in an upcoming release. To migrate:

  - **SchemaHandler.infer_aito_types_from_pandas_series** -> :py:func:`aito.schema.AitoDataTypeSchema.infer_from_samples`
  - **SchemaHandler.infer_table_schema_from_pandas_data_frame** -> :py:func:`aito.schema.AitoTableSchema.infer_from_pandas_dataframe`
  - **SchemaHandler.validate_table_schema** -> :py:func:`aito.schema.AitoTableSchema.from_deserialized_object`


CLI
^^^
- Removed the ``database`` command. All the database operations are now exposed as follows:

  - **aito database quick-add-table** -> **aito quick-add-table**
  - **aito database create-table** -> **aito create-table**
  - **aito database delete-table** -> **aito delete-table**
  - **aito database delete-database** -> **aito delete-database**
  - **aito database upload-entries** -> **aito upload-entries**
  - **aito database upload-file** -> **aito upload-file**
  - **aito database upload-data-from-sql** -> **aito upload-data-from-sql**
  - **aito database quick-add-table-from-sql** -> **aito quick-add-table-from-sql**

- Added the following commands:

  - ``configure``: configure your Aito instance
  - ``get-table``: return the schema of the specified table
  - ``show-tables``: show the existing tables in the Aito instance
  - ``copy-table``: copy a table
  - ``rename-table``: rename a table
  - ``get-database``: return the schema of the database

- Removed dotenv file support (**-e** flag).

0.2.2
-----

- Added missing import warnings to Aito client.
- Updated AitoClient API documentation.

0.2.1
-----

- :py:class:`~aito.client.AitoClient` :py:func:`~aito.client.AitoClient.upload_entries` now accepts `generators`_ as well as lists.

- :py:class:`~aito.client.AitoClient` **upload_entries_by_batches** is deprecated and will be removed in an upcoming release, use :py:func:`~aito.client.AitoClient.upload_entries` instead.


0.2.0
-----

CLI
^^^

- Added a version flag (``--version``) and verbosity level flags (``--verbose`` and ``--quiet``) to the CLI.
- The CLI now returns more concise error messages. Use ``--verbose`` mode if you want to see the comprehensive error message with stack info.
- The ODBC driver name for SQL functions is now specified by an environment variable (``SQL_DRIVER``) or a flag (``--driver``) instead of a required argument as before. For example::

    $ aito infer-table-schema from-sql --driver "PostgreSQL Unicode" "SELECT * FROM tableName"....

  instead of::

    $ aito infer-table-schema from-sql PostgreSQL Unicode" "SELECT * FROM tableName"....


SDK
^^^

- Renamed the ``utils`` package to ``sdk``. Please change the import statement accordingly. For example::

    from aito.sdk.aito_client import AitoClient

- Changes in AitoClient:

  - The class now requires the instance URL (the ``instance_url`` argument) instead of  the instance name (the ```instance_name``` argument).
  - Improve error handling to use Aito response error message.
  - Remove the ``async_same_requests`` function.
  - Rename the arguments of the ``async_request`` function:

    - request_methods -> methods
    - request_paths -> endpoints
    - request_data -> queries

  - ``async_request`` now returns errors if some requests failed.
  - Rename the arguments of the ``request`` function:

    - req_method -> method
    - path -> endpoint
    - data -> query

  - Rename the following functions:

    - put_table schema -> create_table
    - put_database_schema -> create_database
    - check_table_existed -> check_table_exists
    - populate_table_entries -> upload_entries
    - populate_table_entries_by_batches -> upload_entries_by_batches
    - populate_table_by_file_upload -> upload_binary_file
    - query_table_entries -> query_entries

  - Add ``upload_file`` function to upload a file using the its path instead of its file object
  - Add ``optimize_table`` function and add optimize option after data upload
  - Add `Job <https://aito.ai/docs/api/#post-api-v1-jobs-query>`_ related functions: ``create_job``, ``get_job_status``, ``get_job_result``, and ``job_request``
  - Add `Query <https://aito.ai/docs/api/#post-api-v1-query>`_ related functions: ``get_table_size``, ``query_entries``, ``query_all_entries``, and ``download_table``

0.1.2
-----

- Fix a bug when converting or uploading a file in Windows due to tempfile permission
- Fix a bug that requires conversion between String and Text column
- Add compatibility with Python 3.7 and 3.8

0.1.1
-----

- Fix a bug that requires database name for sql\_function
- No longer requires both read-only and read-write key for setting up the credentials.
   (Use ``AITO_API_KEY`` instead of ``AITO_RW_KEY`` and ``AITO_RO_KEY``)

0.1.0
-----

- Integration with SQL. You can now infer table schema, upload data,
   quick add table from the result of a SQL query.

Supported database:

- Postgres
- MySQL

0.0.4
-----

- Change ``client`` task to ``database`` task
- Requires Aito instance name instead of full URL (use ``-i`` flag instead of ``-u`` flag)
- Support tab completion with arg complete

0.0.3
-----

- Add ``quick-add-table, create-table, delete-table, delete-databse, list`` database operation
- Remove the option to create and use table schema from file-upload
- Convert always use standard out
- Improved documentation

.. _aiohttp ClientSession: https://docs.aiohttp.org/en/stable/client_reference.html#client-session
.. _generators: https://aito-python-sdk.readthedocs.io/en/latest/sdk.html#sdkuploaddata
.. _Column Type: https://aito.ai/docs/api/#schema-column-type
.. _Analyzer: https://aito.ai/docs/api/#schema-analyzer
.. _Delimiter Analyzer: https://aito.ai/docs/api/#schema-delimiter-analyzer
