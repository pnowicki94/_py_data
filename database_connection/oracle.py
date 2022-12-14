import cx_Oracle
from database_connection.connection_parameters import connection_parameters

PATH_INIT_ORACLE_CLIENT = r'C:\Program Files\Oracle\instantclient_21_3'
cx_Oracle.init_oracle_client(lib_dir=PATH_INIT_ORACLE_CLIENT)


class ConnectOracle:

    def __init__(self, database_name):

        config_dict, db_type = connection_parameters(database_name)

        if db_type != 'oracle':
            raise ValueError(f"Niewłaściwy typ bazy danych: {db_type} -> wymagany oracle")

        self.host = config_dict.get('host')
        self.username = config_dict.get('username')
        self.password = config_dict.get('password')
        self.port = config_dict.get('port')
        self.service = config_dict.get('service')
        self.connection = None

    def __enter__(self):
        self.make_connection()
        return self

    def make_connection(self):
        conn_str = '{0}/{1}@{2}:{3}/{4}'.format(self.username, self.password, self.host, self.port, self.service)

        try:
            self.connection = cx_Oracle.connect(conn_str)
            print('---' * 10)
            print('Inicializacja oracle database', self.connection.version, self.host, self.username)
            print('---' * 10)
        except Exception as e:
            print(e)

    @property
    def test_conn(self):

        return True if self.connection else False

    @property
    def cursor(self):
        print('create cursor')
        return self.connection.cursor()

    def select_from(self, sql_query):

        return self.cursor.execute(sql_query).fetchall() if self.test_conn else None

    def close_connection(self):

        if self.test_conn:
            self.connection.close()
            self.connection = None
            print('---' * 10)
            print('disconnect oracle database', self.host, self.username)
            print('---' * 10)

    def __exit__(self, exception_type, exception_val, trace):
        self.close_connection()

    def __repr__(self):
        return f"{type(self).__name__} (host: {self.host}, database: '{self.username}')"


def get_dictionary_values(database_name, tables):
    dict_table_values = {}

    with ConnectOracle(database_name) as ora:

        if ora.test_conn:

            cursor = ora.cursor

            for table in tables:

                table_query = exceptions.get(table.lower(), table)

                sql_query = f"select id from {table_query}"

                try:
                    cursor.execute(sql_query)
                except Exception as e:
                    print(e, sql_query)
                    continue

                values = [value[0] for value in cursor.fetchall()]
                set_values = set(values)
                dict_table_values[table] = set_values

    return dict_table_values


def get_max_value_from_field_table(database_name, username, table, field):
    max_value = 0

    with ConnectOracle(database_name) as ora:
        if ora.test_conn:
            cursor = ora.cursor

            sql = f"select MAX({field}) from {username}.{table}"

            try:
                cursor.execute(sql)
                max_value = cursor.fetchone()[0]
            except Exception as e:
                print(f'{e} {table} {field} {max_value}')

    return max_value


def check_if_table_in_view(database_name, username, table):
    with ConnectOracle(database_name) as ora:
        if ora.test_conn:
            cursor = ora.cursor

            sql = f"select count(*) from all_views where owner = '{username}' and view_name like '{table}'"

            try:
                cursor.execute(sql)
                value = cursor.fetchone()[0]

                if value > 1:
                    raise ValueError(f'{table}: {value} widoki. Do weryfikacji')

                if_view = True if value == 1 else False

            except ValueError as e:
                print(e)
                if_view = False
            except Exception as e:
                print(f'{table}: {e}')
                if_view = False

    return if_view
