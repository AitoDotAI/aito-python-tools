### Change Logs
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
