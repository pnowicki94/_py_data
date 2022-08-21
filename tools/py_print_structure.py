"""
Print struktury z baz danych
"""
import arcpy
import os


def print_table_structure(input_database, input_schema, table):
    path_in = input_database + os.sep + input_schema + '.' + table if input_schema else input_database + os.sep + table
    for field_name in [f.name for f in arcpy.ListFields(path_in)]:
        print(table, field_name)


if __name__ == '__main__':
    database = r''
    schema = None
    tables = ['']

    for table_name in tables:
        print_table_structure(database, schema, table_name)
