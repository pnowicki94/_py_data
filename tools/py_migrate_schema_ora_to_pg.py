# -*- coding: utf-8 -*-
"""
Migracja schematów tabel z bazy Oracle do bazy PostgreSQL
"""
from database_connection.postgres import ConnectPostgres
from database_connection.oracle import ConnectOracle
import os

# mapping parameters ---------------------------------------------------------------------------------------------------
mapping_datatype = {
    'CHAR': 'varchar',
    'NCHAR': 'varchar',
    'VARCHAR': 'varchar',
    'VARCHAR2': 'varchar',
    'NVARCHAR2': 'varchar',
    'CLOB': 'varchar',
    'NCLOB': 'varchar',
    'LONG': 'varchar',
    'RAW': 'bytea',
    'BLOB': 'bytea',
    'BFILE': 'bytea',
    'LONG RAW': 'bytea',
    'NUMBER': 'int8',
    'NUMBER(n,m)': 'numeric',
    'FLOAT': 'numeric',
    'BINARY_FLOAT': 'numeric',
    'BINARY_DOUBLE': 'numeric',
    'DATE': 'date',
    'TIMESTAMP': 'date',
    'TIMESTAMP(6)': 'date',
    'TIMESTAMP WITH TIME ZONE': 'date',
    'TIMESTAMP WITH': 'date',
    'INTERVAL YEAR TO MONTH': 'interval',
    'INTERVAL DAY TO SECOND': 'interval'
}

mapping_geomtype = {
    'ST_LINESTRING': 'LINESTRING',
    'ST_POLYGON': 'POLYGON',
    'ST_POINT': 'POINT',
    'ST_MULTIPOLYGON': 'MULTIPOLYGON',
    'ST_MULTIPOINT': 'MULTIPOINT',
}


def migrate_schema_ora_to_pg(_ora, _pg, _schema_pg, _list_tables_schema, _tables, _tables_exceptions):

    tables_columns = {}

    for i, table in enumerate(_list_tables_schema):

        if table[0] in tables_columns:
            tables_columns[table[0]][table[1]] = [table[2], table[3]]
        else:
            tables_columns[table[0]] = {table[1]: [table[2], table[3]]}

    sql_create_all = """"""

    for table, fields in tables_columns.items():

        if _tables and table not in _tables:
            continue

        if table in _tables_exceptions:
            continue

        print(table)

        sql_create = f"""create table if not exists {_schema_pg}.{table.lower()} ("""

        for k, v in fields.items():

            if v[0] == 'ST_GEOMETRY':
                geom_type_sql = f"""select sde.st_geometrytype({k}) from {table} where {k} is not null and rownum = 1"""

                geom_type = _ora.select_from(geom_type_sql)

                if not geom_type:
                    sql_create += f'\n{k.lower()} geometry null,'
                else:
                    sql_create += f'\n{k.lower()} geometry({mapping_geom[geom_type[0][0]]}, 2180) null,'

                pass
            else:

                sql_create += f'\n{k.lower()} {mapping_datatype.get(v[0])}({v[1]}) null,' if v[
                    1] else f'\n{k.lower()} {mapping_datatype.get(v[0])} null,'

        sql_create = sql_create[:-1] + ');\n'

        try:
            if _pg.test_conn:
                _pg.execute_sql(sql_create)
                print('create', table)
        except Exception as e:
            print('error', e, table)

        sql_create_all += sql_create + '\n'

    return sql_create_all


if __name__ == '__main__':
    # tabele do przeniesienia ------------------------------------------------------------------------------------------
    tables = []  # lista tabel do założenia na bazie
    tables_exceptions = []  # lista tabel do pominięcia

    db_ora = ''
    schema_ora = ''

    db_pg = ''
    schema_pg = ''

    sql_dir = r''

    ora = ConnectOracle(db_ora)
    pg = ConnectPostgres(db_pg, schema_pg)

    pg.make_connection()  # skrypt założy tabele na bazie (wykomentowane wygeneruje tylko skrypt z create table)
    ora.make_connection()

    if ora.test_conn:
        select_schema = f"""SELECT table_name, column_name,data_type,char_length FROM all_tab_columns 
                            WHERE owner = '{schema_ora.upper()}' 
                            and table_name not like 'APP_%' 
                            and table_name not like 'KEYSET_%' 
                            and table_name not like '%_IDX$'
                            order by table_name, column_id"""

        list_tables_schema = ora.select_from(select_schema)
        print(list_tables_schema)
        script = migrate_schema_ora_to_pg(ora, pg, schema_pg, list_tables_schema, tables, tables_exceptions)

        filename = f'sql_create_tables_from_{ora.username}_to_{pg.schema}.sql'
        with open(sql_dir + os.sep + filename, 'w') as f_sql:
            f_sql.write(script)

    ora.close_connection()
    pg.close_connection()
