Changelog
=========

0.2.1
-----

- upload_entries now accepts genetors as well as lists
- upload_entries_by_batches has been deprecated


0.2.0
-----

CLI
^^^

- Add a version flag (``--version``) and verbosity level flags (``--verbose`` and ``--quiet``) to the CLI.
- The CLI now returns more concise error messages. Use ``--verbose`` mode if you want to see the comprehensive error message with stack info.
- The ODBC driver name for SQL functions is now specified by an environment variable (``SQL_DRIVER``) or a flag (``--driver``) instead of a required argument as before. For example::

    $ aito infer-table-schema from-sql --driver "PostgreSQL Unicode" "SELECT * FROM tableName"....

  instead of::

    $ aito infer-table-schema from-sql PostgreSQL Unicode" "SELECT * FROM tableName"....


SDK
^^^

- Rename the ``utils`` package to ``sdk``. Please change the import statement accordingly. For example::

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
