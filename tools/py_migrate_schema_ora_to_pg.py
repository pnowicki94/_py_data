# -*- coding: utf-8 -*-
"""
Migracja schematów tabel z bazy Oracle do bazy PostgreSQL
"""
from database_connection.postgres import ConnectPostgres
from database_connection.oracle import ConnectOracle
import time


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

# tabele do przeniesienia ----------------------------------------------------------------------------------------------
tables = []

db_ora = ''
schema_ora = ''

db_pg = ''
schema_pg = ''

ora = ConnectOracle(db_ora)
pg = ConnectPostgres(db_pg, schema_pg)

pg.make_connection()

select_schema = f"""SELECT table_name, column_name,data_type,char_length FROM all_tab_columns 
                    WHERE owner = '{schema_ora}' 
                    and table_name not like 'APP_%' 
                    and table_name not like 'KEYSET_%' 
                    and table_name not like '%_IDX$'
                    order by table_name, column_id"""

ora.make_connection()
list_tables = ora.select_from(select_schema) if ora.test_conn else []

dict_column = {}
dict_tables = {}

start = time.time()
for i, table in enumerate(list_tables):

    if i == 0:
        dict_column[table[1]] = [table[2], table[3]]

    elif table[0] == list_tables[i - 1][0]:
        dict_column[table[1]] = [table[2], table[3]]

    else:

        dict_tables[list_tables[i - 1][0]] = dict_column
        dict_column = {}
        dict_column[table[1]] = [table[2], table[3]]

start_create = time.time()

for table, fields in dict_tables.items():

    if table not in tables:
        continue

    sql_create = f"""create table if not exists {schema_pg}.{table.lower()} ("""

    for k, v in fields.items():

        if v[0] == 'ST_GEOMETRY':
            geom_type_sql = f"""select sde.st_geometrytype({k}) from {table} where {k} is not null and rownum = 1"""

            geom_type = conn_ora.select_from(geom_type_sql)

            if not geom_type:
                sql_create += f'\n{k.lower()} geometry null,'
            else:
                sql_create += f'\n{k.lower()} geometry({mapping_geom[geom_type[0][0]]}, 2180) null,'

            pass
        else:

            sql_create += f'\n{k.lower()} {mapping_datatype.get(v[0])}({v[1]}) null,' if v[
                1] else f'\n{k.lower()} {mapping_datatype.get(v[0])} null,'

    sql_create = sql_create[:-1] + ');'

    try:
        if pg.test_conn:
            pg.execute_sql(sql_create)
            print('create', table)
    except Exception as e:
        print('error', e, table)

    print(sql_create)

print('\ncreate time:', time.time() - start_create)

ora.disconnect()
pg.disconnect()
print('\nall time:', time.time() - start)
