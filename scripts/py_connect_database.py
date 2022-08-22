from database_connection.postgres import ConnectPostgres

"""
PostgreSQL
"""
database_name = 'pg'
schema_name = 'test'

with ConnectPostgres(database_name, schema_name) as pg:
    if pg.test_conn:
        cursor_pg = pg.cursor
        cursor_pg.close()

pg = ConnectPostgres(database_name, schema_name)
pg.make_connection()
if pg.test_conn:
    cursor_pg = pg.cursor
    cursor_pg.close()

pg.close_connection()
