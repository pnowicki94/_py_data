"""
delete data from sde or gdb database
"""
import arcpy
import os


def delete_from_sde_or_gdb(path_to_old_data, path_sde_or_gdb, user_database, _where, _test):
    arcpy.env.workspace = path_to_old_data

    tables = arcpy.ListTables() + arcpy.ListFeatureClasses()

    for table in tables:
        print(table)

        delete_guids = set()
        with arcpy.da.SearchCursor(table, ['ID']) as cursor:
            for row in cursor:
                delete_guids.add(row[0])

        if not delete_guids:
            continue

        print('do usuniecia:', len(delete_guids))
        n = 0
        if user_database:
            path_table = path_sde_or_gdb + os.sep + user_database + '.' + table
        else:
            path_table = path_sde_or_gdb + os.sep + table
        print(path_table)
        with arcpy.da.UpdateCursor(path_table, ['ID'], _where) as u_cursor:
            for u_row in u_cursor:
                if u_row[0] in delete_guids:
                    if _test:
                        n += 1
                    else:
                        u_cursor.deleteRow()
                        n += 1

        print('usunieto:', n)


if __name__ == '__main__':
    # wskazanie bazy danych, na której chcemy wyczyścić dane
    db = ''
    test = False  # True: test, False: delete data

    deleted_database = {'': (r'', '')}

    database, username = deleted_database[db]

    path_gdb_old = r''
    where = ""

    delete_from_sde_or_gdb(path_gdb_old, database, username, where, test)
