Changelog
~~~~~~~~~

0.0.3
^^^^^

-  Add ``quick-add-table, create-table, delete-table, delete-databse, list`` database operation
-  Remove the option to create and use table schema from file-upload
-  Convert always use standard out
-  Improved documentation

0.0.4
^^^^^

-  Change ``client`` task to ``database`` task
-  Requires Aito instance name instead of full URL (use ``-i`` flag instead of ``-u`` flag)
-  Support tab completion with arg complete

0.1.0
^^^^^

-  Integration with SQL. You can now infer table schema, upload data,
   quick add table from the result of a SQL query.

Supported database:

- Postgres
- MySQL

0.1.1
^^^^^

-  Fix a bug that requires database name for sql\_function
-  No longer requires both read-only and read-write key for setting up the credentials.
   (Use ``AITO\_API\_KEY`` instead of ``AITO\_RW\_KEY`` and ``AITO\_RO\_KEY``)

0.1.2
^^^^^

-  Fix a bug when converting or uploading a file in Windows due to tempfile permission
-  Fix a bug that requires conversion between String and Text column
-  Add compatibility with Python 3.7 and 3.8
