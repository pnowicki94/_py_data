from sqlalchemy import create_engine
from database_connection.connection_parameters import connection_parameters
from tools.tools import try_except, write_log


class ConnectPostgres:

    def __init__(self, database_name, schema):
        """

        :param database_name: nazwa pliku z konfiguracją połączenia do bazy danych
        """

        config_dict, db_type = connection_parameters(database_name)

        if db_type != 'postgres':
            raise ValueError(f"Niewłaściwy typ bazy danych: {db_type} -> wymagany postgres")

        self.database = config_dict.get('database')
        self.host = config_dict.get('host')
        self.username = config_dict.get('username')
        self.password = config_dict.get('password')
        self.port = config_dict.get('port')
        self.schema = schema
        self.engine, self.connection = None, None

    def __enter__(self):

        return self.make_connection

    @property
    def make_connection(self):

        str_conn = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

        try:
            self.engine = create_engine(str_conn)
            self.connection = self.engine.raw_connection()
            print(f"Connect database postgres: {self.host} - {self.database}")
            return self
        except Exception as e:
            print(e)

        return self

    def close_connection(self):

        if self.connection:
            self.connection.close()
            print('disconnect database postgres')

    def __exit__(self, exception_type, exception_val, trace):
        self.close_connection()

    @property
    def cursor(self):
        print('create cursor')
        return self.connection.cursor()

    def __repr__(self):
        return f"{type(self).__name__} (host: {self.host}, database: '{self.database}')"


@try_except
def valid_database_postgres(database_name, schema, relations):
    with ConnectPostgres(database_name, schema) as conn_pg:
        if conn_pg.connection:

            cursor = conn_pg.cursor()

            sql_tables = f"""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = '{conn_pg.schema}'
                    ORDER BY table_name
                    """

            cursor.execute(sql_tables)

            tables_database = [value[0].lower() for value in cursor.fetchall()]
            tables_database = set(tables_database)

            for table, relation in relations.items():

                if table.lower() not in tables_database:
                    continue

                for rel in relation:

                    if not rel:
                        continue

                    sql_query = f"select id from {conn_pg.schema}.{table}"

                    cursor.execute(sql_query)

                    set_guids = set([value[0] for value in cursor.fetchall()])

                    for table_rel, field_rel in rel.items():

                        if table_rel.lower() not in tables_database:
                            continue

                        if isinstance(field_rel, list):
                            for field_r in field_rel:
                                sql_query = f"""select {field_r} from {conn_pg.schema}.{table_rel} 
                                                where {field_r} is not null"""
                                cursor.execute(sql_query)

                                records, errors, data = validate_data(set_guids, [v[0] for v in cursor.fetchall()])
                                if errors:
                                    print(f'{table_rel} {field_r} --> {table} id: records: {records}, errors: {errors}')
                                    file_name = f'{table}__{table_rel}__{field_r}__{records}__{errors}.log'
                                    write_log('logMigrateValue', file_name, data)

                        else:

                            sql_query = f"""select {field_rel} from {conn_pg.schema}.{table_rel} 
                                            where {field_rel} is not null"""
                            cursor.execute(sql_query)

                            records, errors, data = validate_data(set_guids, [v[0] for v in cursor.fetchall()])
                            if errors:
                                print(f'{table_rel} {field_rel} --> {table} id: records: {records}, errors: {errors}')
                                file_name = f'{table}__{table_rel}__{field_rel}__{records}__{errors}.log'
                                write_log('logMigrateValue', file_name, data)


@try_except
def get_max_value_from_field_table(database_name, username, table, field):
    max_value = 0

    with ConnectPostgres(database_name, username) as conn_pg:
        if conn_pg.connection:
            cursor = conn_pg.cursor()

            sql = f"select MAX({field}) from {conn_pg.schema}.{table}"

            try:
                cursor.execute(sql)
                max_value = cursor.fetchone()[0]
            except Exception as e:
                print(f'{e} {table} {field} {max_value}')

    return max_value
