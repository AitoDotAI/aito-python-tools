# Aito Command Line Interface SQL Integration

The Aito CLI supports integration between your SQL database and the Aito database

## Supported functions
* Infer a table schema from the result of a SQL query:
  ```bash
  aito infer-table-schema from-sql "PostgreSQL Unicode" "SELECT * FROM tableName" > inferredSchema.json
  ```
* Upload the result of a SQL to an existing table:
  ```bash
  aito database upload-data-from-sql "MySQL ODBC 8.0 Driver" tableName "SELECT * FROM tableName"
  ```
* Infer schema, create table, and upload the result of a SQL to the database:
  ```bash
  aito database quick-add-table-from-sql "PostgreSQL Unicode" -s localhost -u root -d testDB -tableName "SELECT * FROM tableName"
  ```

## <a name="installation"> Additional Installation

The Aito CLI uses the python module [pyodbc](https://github.com/mkleehammer/pyodbc) to access to [ODBC](https://docs.microsoft.com/en-us/sql/odbc/reference/what-is-odbc?view=sql-server-ver15) databases.
You need to install:
  * The ODBC driver manager (varies from system, read the instructions below)
  * Install the aitoai package with SQL feature
    ```bash
    pip install aitoai[SQL]
    ```
    or install the pyodbc package on top of the original aitoai package
    ```bash
    pip install aitoai
    pip install pyodbc
    ```
  * Your database specific ODBC driver

to enable the CLI's sql integration functions.
More instructions regarding the pyodbc library and connecting to different databases can be found [here](https://github.com/mkleehammer/pyodbc/wiki)

#### Install the ODBC Driver Manager:
* Ubuntu:
  ```bash
  sudo apt update
  sudo apt install unixodbc
  sudo apt install unixodbc-dev
  ```
* Mac:
  ```bash
  brew update
  brew install unixodbc freetds
  ```
* Generic Linux:
  ```
  sudo yum check-update
  sudo yum install unixODBC.x86_64
  sudo yum install unixODBC-devel.x86_64
  ```

#### Install PostgreSQL ODBC Driver:
The official instructions can be found [here](https://odbc.postgresql.org/)
* Ubuntu:
  ```bash
  sudo apt install odbc-postgresql
  ```
* Mac:
  ```bash
  brew install psqlodbc
  ```
* Generic Linux:
  ```bash
  sudo yum install postgresql-odbc.x86_64
  ```

#### Install MySQL ODBC Driver:
The official instructions can be found [here](https://dev.mysql.com/doc/connector-odbc/en/connector-odbc-installation.html)
  * Download the appropriate binary installer for your OS [here](https://dev.mysql.com/downloads/connector/odbc/).
  The example below is for Ubuntu 18.04
  ```bash
  wget https://dev.mysql.com/get/Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.18-linux-ubuntu18.04-x86-64bit.tar.gz
  ```
  * Binary installation by:
  ```
  tar zxvf mysql-connector-odbc-8.0.18-linux-ubuntu18.04-x86-64bit.tar.gz
  sudo cp mysql-connector-odbc-8.0.18-linux-ubuntu18.04-x86-64bit.tar.gz/bin/* /usr/local/bin
  sudo cp mysql-connector-odbc-8.0.18-linux-ubuntu18.04-x86-64bit.tar.gz/lib/* /usr/local/lib
  sudo chmod 777 /usr/local/lib/libmyodbc*
  sudo myodbc-installer -a -d -n "MySQL ODBC 8.0 Driver" -t "Driver=/usr/local/lib/libmyodbc8w.so"
  ```
  * Verify that the driver is installed and registered:
  ```bash
  myodbc-installer -d -l
  ```

#### Setting up the credentials to your SQL database
There are 3 ways to set up the credentials:
* The most convenient way is to set up the following environment variables:
  ```bash
  SQL_SERVER=server to connect to
  SQL_PORT=port to connect to
  SQL_DATABASE=database_to_connect_to
  SQL_USER=username for authentication
  SQL_PASSWORD=password for authentication
  ```

  You can now perform the sql operations. For example:
  ```bash
  aito infer-table-schema from-sql "PostgreSQL Unicode" "SELECT * FROM table"
  ```
* Using a dotenv (```.env```) file:

  *Your .env file should contain environment variables as described above.*

  You can set up the credentials using a dotenv file with the `-e` flag. For example:

  ```bash
  aito infer-table-schema from-sql -e path/to/myDotEnvFile.env "PostgreSQL Unicode" "SELECT * FROM table"
  ```
* Using flags:

  You can set up the credentials using `-s` flag for the server, `-P` flag for the port, `-d` flag for the database, `-u` flag for the username, and `-p` for the password
