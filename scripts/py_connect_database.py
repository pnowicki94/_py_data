from database_connection.postgres import ConnectPostgres
from database_connection.oracle import ConnectOracle
"""
PostgreSQL
"""
database_name = 'test'
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

"""
Oracle
"""

database_name = 'test'
with ConnectOracle(database_name) as ora:
    if ora.test_conn:
        cursor_ora = ora.cursor
        del cursor_ora
