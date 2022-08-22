import cx_Oracle
from database_connection.connection_parameters import connection_parameters
from tools.tools import try_except, write_log

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


@try_except
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


@try_except
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


def get_view_ddl(database_name, username, table):
    with ConnectOracle(database_name) as ora:
        if ora.test_conn:
            cursor = ora.cursor

            sql = f"""select dbms_metadata.get_ddl('VIEW', VIEW_NAME, '{username}') from ALL_VIEWS WHERE 
                        OWNER = '{username}' and VIEW_NAME like '%{table}%'"""

            try:
                cursor.execute(sql)
                view_ddl = cursor.fetchone()[0]
            except Exception as e:
                print(f'{table}: {e}')
                view_ddl = None

    return view_ddl


def get_views_dependencies(database_name, username):
    """ Zależności widoków od tabel dla danego użytkownika bazy danych

    :param database_name: nazwa bazy danych
    :param username: nazwa użytkownika/schematu
    :return: views_dependencies key: nazwa tabeli, value: lista widoków; table_view key: nazwa_tabeli, value: nazwa_widoku
    """

    exceptions_views_name = ()
    testing_views_name = ''

    with ConnectOracle(database_name) as ora:
        if ora.test_conn:
            cursor = ora.cursor

            sql = f"""select name, referenced_name from all_dependencies where 
                        owner = '{username}' and 
                        type = 'VIEW' 
                        order by referenced_name"""

            try:
                cursor.execute(sql)
                views_depend = cursor.fetchall()
            except Exception as e:
                print(f'{e}')
                views_depend = {}

            views_dependencies = {}
            for view, dependencies in views_depend:

                if view in exceptions_views_name or testing_views_name in view:
                    continue

                variants = {}

                dependencies = variants.get(dependencies, dependencies)

                if dependencies in views_dependencies:
                    values = views_dependencies[dependencies]
                    values.add(view)
                else:
                    views_dependencies[dependencies] = set([])
                    views_dependencies[dependencies].add(view)

            table_view = {}
            for table, views in views_dependencies.items():
                for view in views:
                    if table in view:

                        if table in table_view:
                            values = table_view[table]
                            values.add(view)
                        else:
                            table_view[table] = set([])
                            table_view[table].add(view)

    return views_dependencies, table_view
