### Changelog
#### 0.0.3
* Add ```quick-add-table, create-table, delete-table, delete-databse, list``` client operation
* Remove the option to create and use table schema from file-upload
* Convert always use standard out
* Improved documentation

#### 0.0.4
* Change `client` task to `database` task
* Requires Aito instance name instead of full URL (use `-i` flag instead of `-u` flag)
* Support tab completion with arg complete

#### 0.1.0
* Integration with SQL. You can now infer table schema, upload data, quick add table from the result of a SQL query.

  Supported database:
  * Postgres
  * MySQL

#### 0.1.1
* Fix a bug that requires database name for sql_function
* No longer requires specification of RO or RW key (Use AITO_API_KEY instead of AITO_RW_KEY and AITO_RO_KEY)

#### 0.1.2
* Fix a bug when converting or uploading a file in Windows due to tempfile persmission
* Fix a bug that requires conversion between String and Text column
* Add compatibility with Python 3.7 and 3.8