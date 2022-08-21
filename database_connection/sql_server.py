import pyodbc
from database_connection.connection_parameters import connection_parameters


class Connect:

    def __init__(self, database_name):
        """

        :param database_name: nazwa pliku z konfiguracją połączenia do bazy danych
        """

        config_dict, db_type = connection_parameters(database_name)

        if db_type != 'mssql':
            raise ValueError(f"Niewłaściwy typ bazy danych: {db_type} -> wymagany mssql")

        self.database = config_dict.get('database')
        self.host = config_dict.get('host')
        self.username = config_dict.get('username')
        self.password = config_dict.get('password')
        self.port = config_dict.get('port')

    def __enter__(self):

        try:
            print(rf'Driver=SQL Server;Server={self.host};Database={self.database};Trusted_Connection=yes;')
            self.connection = pyodbc.connect(
                rf'Driver=SQL Server;Server={self.host};Database={self.database};Trusted_Connection=yes;')
        except Exception as err:
            self.connection = None
            print(err)

        return self

    def __close(self):

        if self.connection:
            self.connection.close()
            print('disconnect database postgres')

    def __exit__(self, exception_type, exception_val, trace):
        self.__close()

    def cursor(self):

        return self.connection.cursor()

    def __repr__(self):
        return f"{type(self).__name__} (host: {self.host}, database: '{self.database}')"
