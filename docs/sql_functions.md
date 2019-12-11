# Aito Command Line Interface SQL Integration

The Aito CLI supports integration between your SQL database and the Aito database

## <a name="installation"> Additional Installation

The Aito CLI uses the python module [pyodbc](https://github.com/mkleehammer/pyodbc) to access to [ODBC](https://docs.microsoft.com/en-us/sql/odbc/reference/what-is-odbc?view=sql-server-ver15) databases.
You need to install:
  * The ODBC driver manager (varies from system)
  * The pyodbc module with ```pip install pyodbc```.
  * The database specific driver

to enable the CLI's sql integration functions.
More instructions regarding the pyodbc library and connecting to different databases can be found [here](https://github.com/mkleehammer/pyodbc/wiki)

#### CentOS:
* Install the unixODBC driver manager:
  ```bash
  sudo yum check-update
  sudo yum install unixODBC.x86_64
  sudo yum install unixODBC-devel.x86_64
  ```
* Install PostgreSQL ODBC driver:
  ```bash
  sudo yum install postgresql-odbc.x86_64
  ``````

#### Ubuntu:
* Install the unixODBC driver manager:
  ```bash
  sudo apt update
  sudo apt install unixodbc
  sudo apt install unixodbc-dev
  ```
* Install PostgreSQL ODBC driver:
  ```bash
  sudo apt install odbc-postgresql
  ```

#### Mac:
* Install the unixODBC driver manager:
  ```bash
  brew update
  brew install unixodbc freetds
  ```
* Install PostgreSQL ODBC driver:
  ```bash
  brew install psqlodbc
  ```

## Supported functions
#### Setting up the credentials to your SQL database
There are 3 ways to set up the credentials:
* The most convinient way is to set up the following environment variables:
  ```
  SERVER=server to connect to
  PORT=port to connect to
  DATABASE=database_to_connect_to
  USER=username for authentication
  PWD=password for authentication
  ```

  You can now perform the sql operations. For example:
  ```bash
  aito  infer-table-schema from-sql postgres "SELECT * FROM table"
  ```
* Using a dotenv (```.env```) file:

  *Your .env file should contain environment variables as described above.*

  You can set up the credentials using a dotenv file with the `-e` flag. For example:

  ```bash
  aito infer-table-schema from-sql -e path/to/myDotEnvFile.env postgres "SELECT * FROM table"
  ```
* Using flags:

  You can set up the credentials using `-s` flag for the server, `-P` flag for the port, `-d` flag for the database, `-u` flag for the username, and `-p` for the password

#### Supported functions:
* Infer a table schema from the result of a SQL query:
  ```bash
  aito infer-table-schema from-sql postgres "SELECT * FROM tableName" > inferredSchema.json
  ```
* Upload the result of a SQL to an existing table:
  ```bash
  aito database upload-data-from-sql postgres tableName "SELECT * FROM tableName"
  ```
* Infer schema, create table, and upload the result of a SQL to the database:
  ```bash
  aito database quick-add-table-from-sql postgres tableName "SELECT * FROM tableName"
  ```
