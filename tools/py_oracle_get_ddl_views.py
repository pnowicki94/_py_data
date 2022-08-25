from database_connection.oracle import ConnectOracle
import os


def get_ddl_view(database_name, _username, tables):
    with ConnectOracle(database_name) as ora:
        if ora.test_conn:
            cursor = ora.cursor
            views_ddl = ""
            for table in tables:
                print(table)

                sql = f"""select dbms_metadata.get_ddl('VIEW', VIEW_NAME, '{_username}') from ALL_VIEWS WHERE 
                            OWNER = '{_username}' and VIEW_NAME like '%{table}%'"""

                try:
                    cursor.execute(sql)
                    view_ddl = str(cursor.fetchone()[0]) + ';\n'
                except Exception as e:
                    print(f'{table}: {e}')
                    view_ddl = ""

                views_ddl += view_ddl

    return views_ddl


if __name__ == '__main__':
    views = [
        ''
    ]

    db = ''
    username = ''
    dir_files = r''
    sql_ddl = get_ddl_view(db, username, views)

    with open(dir_files + os.sep + 'sql_ddl_views.sql', 'w') as f:
        f.write(sql_ddl)
