from database_connection.postgres import ConnectPostgres

"""
PostgreSQL
"""
database_name = 'pg'
schema_name = 'test'

with ConnectPostgres(database_name, schema_name) as conn_pg:
    if conn_pg.connection:
        cursor = conn_pg.cursor

pg = ConnectPostgres(database_name, schema_name)
conn_pg = pg.make_connection
if conn_pg.connection:
    cursor_pg = conn_pg.cursor

conn_pg.close_connection()
