import argparse
import sys


class ArgParser(argparse.ArgumentParser):
    def __init__(self, **kwargs):
        super().__init__(formatter_class=argparse.RawTextHelpFormatter, **kwargs)

    def error(self, message):
        sys.stderr.write(f"error: {message}\n")
        self.print_help()
        sys.exit(2)

    def add_aito_default_credentials_arguments(self):
        args = self.add_argument_group("aito credential arguments")
        args.add_argument(
            '-e', '--use-env-file', type=str, metavar='env-input-file',
            help='set up the credentials using a .env file containing the required env variables'
        )
        args.add_argument('-i', '--instance-url', type=str, default='.env', help='specify aito instance url')
        args.add_argument(
            '-k', '--api-key', type=str, default='.env', help='specify aito read-write or read-only API key'
        )
        epilog_str = '''\
        You must provide your Aito credentials to execute database operations
          If no Aito credential flag is given, the following environment variables are used:
          AITO_INSTANCE_URL, AITO_API_KEY
        '''
        if not self.epilog:
            self.epilog = epilog_str
        else:
            self.epilog += epilog_str

    def add_sql_default_credentials_arguments(self, add_use_env_arg: bool = False):
        """sql connection default arguments

        :param add_use_env_arg: disable use env file to avoid conflict with database aito credentials arguments
        :return:
        """
        args = self.add_argument_group('database connection arguments')
        if add_use_env_arg:
            args.add_argument('-e', '--use-env-file', type=str, metavar='env-input-file',
                              help='set up the credentials using a .env file containing the required env variables')

        args.add_argument('--driver', '-D', type=str, help='the name of the ODBC driver', default='.env')
        args.add_argument('--server', '-s', type=str, help='server to connect to', default='.env')
        args.add_argument('--port', '-P', type=str, help='port to connect to', default='.env')
        args.add_argument('--database', '-d', type=str, help='database to connect to', default='.env')
        args.add_argument('--username', '-u', type=str, help='username for authentication', default='.env')
        args.add_argument('--password', '-p', type=str, help='password for authentication', default='.env')

        epilog_str = '''\
        If no database credentials flag is given, the following environment variable are used: 
          SQL_DRIVER, SQL_SERVER, SQL_PORT, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD
        '''
        if not self.epilog:
            self.epilog = epilog_str
        else:
            self.epilog += epilog_str

    def add_csv_format_default_arguments(self):
        self.add_argument('-d', '--delimiter', type=str, default=',', help="delimiter character(default: ',')")
        self.add_argument('-p', '--decimal', type=str, default='.', help="decimal point character(default '.')")

    def add_excel_format_default_arguments(self):
        self.add_argument('-o', '--one-sheet', type=str, metavar='sheet-name', help='read a sheet by sheet name')
