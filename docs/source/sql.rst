SQL Database Integration
========================

The Aito Python SDK supports integration with your SQL database so that you can upload the result of your SQL query to Aito.

.. _sqlInstallation:

Additional Installation
-----------------------

We use pyodbc_ module to gain access to ODBC_ supported databases.

To enable the SQL integration, you need to do the following extra installation:

- `Install ODBC Driver Manager`_
- Install the pyodbc_ package on top of the original aitoai package::

    $ pip install aitoai
    $ pip install pyodbc

- `Install Database ODBC driver`_
   - :ref:`PostgreSQL<Install PostgreSQL ODBC Driver>`
   - :ref:`MySQL<Install MySQL ODBC Driver>`

More instructions regarding the pyodbc_ library and connecting to different databases can
be found `here <https://github.com/mkleehammer/pyodbc/wiki>`__.

Install ODBC Driver Manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  On Ubuntu::

    $ sudo apt update
    $ sudo apt install unixodbc
    $ sudo apt install unixodbc-dev

-  On OSX::

    $ brew update
    $ brew install unixodbc

-  On Generic Linux::

    $ sudo yum check-update
    $ sudo yum install unixODBC.x86_64
    $ sudo yum install unixODBC-devel.x86_64

Install Database ODBC driver
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Install PostgreSQL ODBC Driver
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

psqlODBC_ is the official PostgreSQL ODBC driver. To install:

- On Ubuntu::

    $ sudo apt install odbc-postgresql

- On OSX::

    $ brew install psqlodbc

- On Generic Linux::

    $ sudo yum install postgresql-odbc.x86_64

Install MySQL ODBC Driver
^^^^^^^^^^^^^^^^^^^^^^^^^

The official instructions can be found `here <https://dev.mysql.com/doc/connector-odbc/en/connector-odbc-installation.html>`_. To install:

- Download and install the appropriate binary installer for your OS `here <https://dev.mysql.com/downloads/connector/odbc/>`__. The example below is for Ubuntu 18.04::

    $ wget https://dev.mysql.com/get/Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.18-linux-ubuntu18.04-x86-64bit.tar.gz
    $ tar zxvf mysql-connector-odbc-8.0.18-linux-ubuntu18.04-x86-64bit.tar.gz
    $ sudo cp mysql-connector-odbc-8.0.18-linux-ubuntu18.04-x86-64bit.tar.gz/bin/* /usr/local/bin
    $ sudo cp mysql-connector-odbc-8.0.18-linux-ubuntu18.04-x86-64bit.tar.gz/lib/* /usr/local/lib
    $ sudo chmod 777 /usr/local/lib/libmyodbc*
    $ sudo myodbc-installer -a -d -n "MySQL ODBC 8.0 Driver" -t "Driver=/usr/local/lib/libmyodbc8w.so"``

- Verify that the driver is installed and registered::

    $ myodbc-installer -d -l


CLI Integration
---------------

Set Up SQL Database Credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Similar to setting up the Aito credentials, there are 3 ways to set up the SQL Database credentials:

1. The most convenient way is to set up the following environment variables::

    $ export SQL_DRIVER=the name of the database ODBC driver
    $ export SQL_SERVER=the server to connect to
    $ export SQL_PORT=the port to connect to
    $ export SQL_DATABASE=the database to connect to
    $ export SQL_USER=the username for authentication
    $ export SQL_PASSWORD=the password for authentication

  You can now perform the sql operations. For example::

    $ aito infer-table-schema from-sql "PostgreSQL Unicode" "SELECT * FROM table

2. Using a dotenv (``.env``) file

  Your .env file should contain environment variables as described above

  You can set up the credentials using a dotenv file with the ``-e`` flag. For example::

    $ aito infer-table-schema from-sql -e path/to/dotEnvFile.env "PostgreSQL Unicode" "SELECT * FROM table"

  .. note::

    For database operation with SQL integration, the dotenv file must also contain the Aito instance credentials.

3. Using flags:

  You can set up the credentials using:

    - ``-D`` flag for the name of the driver
    - ``-s`` flag for the server
    - ``-P`` flag for the port
    - ``-d`` flag for the database
    - ``-u`` flag for the username
    - ``-p`` for the password

Supported Functions
~~~~~~~~~~~~~~~~~~~

- Infer a table schema from the result of a SQL query::

    $ aito infer-table-schema from-sql "SELECT * FROM tableName" > inferredSchema.json

  To see help::

    $ aito infer-table-schema from-sql -h

- Upload the result of a SQL to an existing table::

    $ aito database -e path/to/dotEnvFile.env upload-data-from-sql tableName "SELECT * FROM tableName"

  To see help::

    $ aito database upload-data-from-sql -h

- Infer schema, create table, and upload the result of a SQL to the database::

    $ aito database quick-add-table-from-sql -D "PostgreSQL Unicode" -s localhost -u root -d testDB -tableName "SELECT * FROM tableName"

  To see help::

    $ aito database quick-add-table-from-sql -h


SDK Integration
---------------

You can connect to your SQL Database using the :py:class:`~aito.utils.sql_connection.SQLConnection`. The example below shows how you can upload a SQL query results to an Aito table:

.. code:: python

  from aito.utils.sql_connection import SQLConnection
  connection = SQLConnection(
    sql_driver='PostgreSQL Unicode',
    sql_server='localhost',
    sql_database='database_name',
    sql_usersname='username',
    sql_password='password'
  )

  # save query results to pandas DataFrame
  query_results_df = connection.execute_query_and_save_result(query = 'from table select *')
  # convert DataFrame to list of entries
  query_results_entries = query_results_df.to_dict(orient="records")

  # create aito client
  aito_client = AitoClient(instance_url="your_aito_instance_url", api_key="your_rw_api_key")
  # upload entries to table
  aito_client.upload_entries(table_name='table', entries=query_results_entries)


Troubleshooting
---------------

Database ODBC Driver not found after installation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is possible that the database driver is not registered to the ODBC Driver Manager automatically.
In this case, you have to do it manually by following these steps:

- After installing the ODBC Driver Manager, you should be able to run the following command to check the location of ODBC ini files on your system::

    $ odbcinst -j

  The response should look similar to this::

    unixODBC 2.3.7
    DRIVERS............: /usr/local/etc/odbcinst.ini
    SYSTEM DATA SOURCES: /usr/local/etc/odbc.ini
    FILE DATA SOURCES..: /usr/local/etc/ODBCDataSources
    USER DATA SOURCES..: /User/distiller/.odbc.ini
    SQLULEN Size.......: 8
    SQLLEN Size........: 8
    SQLSETPOSIROW Size.: 8

   You only need to care about the location of the driver ini file, which is ``/usr/local/etc/odbcinst.ini`` in this case.

-  Find the location of the database driver and add it to the driver ini file. For example, the postgres unicode odbc driver is at ``/usr/local/lib/psqlodbcw.so``. Simply append the following text to the driver ini file::

    [PostgreSQL Unicode]
      Driver=/usr/local/lib/psqlodbcw.so

-  You should now be able to connect to your database using the Aito CLI.


.. _pyodbc: https://github.com/mkleehammer/pyodbc
.. _ODBC: https://docs.microsoft.com/en-us/sql/odbc/reference/what-is-odbc?view=sql-server-ver15
.. _psqlODBC: https://odbc.postgresql.org/
