from database_connection.oracle import ConnectOracle


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


if __name__ == 'main':
    db = ''
    user = ''
    dict_views_dependencies, dict_table_view = get_views_dependencies(db, user)
