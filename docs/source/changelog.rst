Changelog
=========

0.3.0
-----

SDK
^^^
- :ref:`apiAitoSchema`: The SDK now supports the component object of Aito schema including

  - :py:class:`~aito.sdk.aito_schema.AitoAnalyzerSchema`
  - :py:class:`~aito.sdk.aito_schema.AitoDataTypeSchema`
  - :py:class:`~aito.sdk.aito_schema.AitoColumnTypeSchema`
  - :py:class:`~aito.sdk.aito_schema.AitoTableSchema`
  - :py:class:`~aito.sdk.aito_schema.AitoDatabaseSchema`

- :ref:`apiSchemaHandler` is deprecated and will be removed in an upcoming release.

  - :meth:`~aito.sdk.schema_handler.SchemaHandler.infer_aito_types_from_pandas_series` -> :meth:`aito.sdk.aito_schema.AitoDataTypeSchema.infer_from_samples`
  - :meth:`~aito.sdk.schema_handler.SchemaHandler.infer_table_schema_from_pandas_data_frame` -> :meth:`aito.sdk.aito_schema.AitoTableSchema.infer_from_pandas_dataframe`
  - :meth:`~aito.sdk.schema_handler.SchemaHandler.validate_table_schema` -> :meth:`aito.sdk.aito_schema.AitoTableSchema.from_deserialized_object`

- Minor changes:

  - Improved `Analyzer`_ inference: can now detect `Delimiter Analyzer`_ and is exposed at :meth:`aito.sdk.aito_schema.AitoAnalyzerSchema.infer_from_samples`
  - :ref:`apiAitoClient` :meth:`~aito.sdk.aito_client.AitoClient.get_table_schema` and :meth:`~aito.sdk.aito_client.AitoClient.get_database_schema` now return the schema object instead of JSON response
  - :ref:`apiAitoClient` :meth:`~aito.sdk.aito_client.AitoClient.create_table`, :ref:`apiDataFrameHandler` :meth:`~aito.sdk.data_frame_handler.DataFrameHandler.convert_df_using_aito_table_schema` and :meth:`~aito.sdk.data_frame_handler.DataFrameHandler.convert_file` now supports input of AitoTableSchema object
  - :ref:`apiAitoClient` :meth:`~aito.sdk.aito_client.AitoClient.query_entries` now returns entries instead of JSON response
  - :ref:`apiAitoClient` :meth:`~aito.sdk.aito_client.AitoClient.query_entries` and :meth:`~aito.sdk.aito_client.AitoClient.query_all_entries` now supports the ``select`` keyword to select the field in an entry


CLI
^^^
- Added the shorthanded ``aitodb`` for the :ref:`cliDatabase` command. You can now perform database operation with ``aitodb <operation>`` instead of ``aito database <operation>``
- Added the following database operations:

  - ``login``: login to your Aito instance
  - ``show-tables``: show the existing tables in the Aito instance
  - ``copy-table``: copy a table
  - ``rename-table``: rename a table

0.2.2
-----

- Added missing import warnings to Aito client.
- Updated AitoClient API documentation.

0.2.1
-----

- :ref:`apiAitoClient` :meth:`~aito.sdk.aito_client.AitoClient.upload_entries` now accepts `generators`_ as well as lists.

- :ref:`apiAitoClient` :meth:`~aito.sdk.aito_client.AitoClient.upload_entries_by_batches` is deprecated and will be removed in an upcoming release, use :meth:`~aito.sdk.aito_client.AitoClient.upload_entries` instead.


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


.. _generators: https://aitodotai.github.io/aito-python-tools/quickstart.html#sdkquickstartuploaddata
.. _Column Type: https://aito.ai/docs/api/#schema-column-type
.. _Analyzer: https://aito.ai/docs/api/#schema-analyzer
.. _Delimiter Analyzer: https://aito.ai/docs/api/#schema-delimiter-analyzer