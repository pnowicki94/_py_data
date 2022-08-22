from tools.tools import try_except, write_log
import numpy as np
import arcpy
import os

PATH_REFERENCE = r'C:\GISPartner\isok\_isok_database\_reference_data\reference_data.gdb'


class EditSession:

    def __init__(self, path):

        arcpy.env.workspace = path
        self.workspace = arcpy.env.workspace

    def __enter__(self):
        try:
            self.edit = arcpy.da.Editor(self.workspace)
            self.edit.startEditing(True, False)  # False, False
            self.edit.startOperation()
            print(self.edit)
        except Exception as e:
            self.edit = None
            print(e)
            raise RuntimeError(e)

        return self.edit

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.edit:
            self.edit.stopOperation()
            self.edit.stopEditing(True)
            print('stopEditing')
        else:
            print('EditSession not start')


class GenerateCursor:

    def __init__(self, path, table, list_fields):

        """
        :param path: ścieżka do pliku gdb
        :param table: tabla w w bazie gdb
        :param list_fields: lista z polami w tabeli
        :return: cursor
        """

        self.path = path
        self.table = table
        self.list_fields = list_fields

    def __enter__(self):
        try:
            self.cursor = arcpy.da.InsertCursor(self.path + os.sep + self.table, self.list_fields)
            print('open cursor')
        except Exception as e:
            self.cursor = None
            print(e)
            raise RuntimeError(e)

        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            del self.cursor
            print('dell cursor')
        else:
            print('cursor not open')


@try_except
def valid_geo_database(geo_database, relations):
    arcpy.env.workspace = geo_database

    tables_geo_database = arcpy.ListTables() + arcpy.ListFeatureClasses()
    tables_geo_database = [t.lower() for t in tables_geo_database]

    for table, relation in relations.items():

        if table.lower() not in tables_geo_database:
            continue

        list_guids = [row[0] for row in arcpy.da.SearchCursor(table, ['ID'])]

        if arcpy.Exists(PATH_REFERENCE + os.sep + table):
            print('exist', PATH_REFERENCE + os.sep + table)
            list_reference = [row[0] for row in arcpy.da.SearchCursor(PATH_REFERENCE + os.sep + table, ['ID'])]
            list_guids += list_reference

        set_guids = set(list_guids)

        for rel in relation:

            if not rel:
                continue

            for table_rel, field_rel in rel.items():

                if table_rel.lower() not in tables_geo_database:
                    continue

                if isinstance(field_rel, list):

                    for field_r in field_rel:

                        if field_r == 'CHILD_ID':
                            values = [row[0] for row in arcpy.da.SearchCursor(table_rel, [field_r],
                                                                              f"{field_r} is not null and CHILD_TABLE_NAME = '{table}'")]
                        else:
                            values = [row[0] for row in
                                      arcpy.da.SearchCursor(table_rel, [field_r], f"{field_r} is not null")]

                        records, errors, data = validate_data(set_guids, values)
                        if errors:
                            print(f'{table_rel} {field_r} --> {table} id: records: {records}, errors: {errors}')
                            file_name = f'{table}__{table_rel}__{field_r}__{records}__{errors}.log'
                            write_log('logMigrateValue', file_name, data)

                else:

                    values = [row[0] for row in
                              arcpy.da.SearchCursor(table_rel, [field_rel], f"{field_rel} is not null")]

                    records, errors, data = validate_data(set_guids, values)
                    if errors:
                        print(f'{table_rel} {field_rel} --> {table} id: records: {records}, errors: {errors}')
                        file_name = f'{table}__{table_rel}__{field_rel}__{records}__{errors}.log'
                        write_log('logMigrateValue', file_name, data)


@try_except
def valid_dictionary_values_geo_database(geo_database, database_name_ora, tables):
    arcpy.env.workspace = geo_database
    tables_geo_database = arcpy.ListTables() + arcpy.ListFeatureClasses()
    tables_geo_database = [t.lower() for t in tables_geo_database]

    dict_tables = set([])
    variants = []

    for table in tables:

        if table.lower() not in tables_geo_database:
            continue

        fields = arcpy.ListFields(table)
        fields = [f.name for f in fields if f.name.startswith("ID_SLO")]

        for field in fields:
            dict_field, dict_table = field, field[3:]
            variants.append([table, dict_field, dict_table])
            dict_tables.add(dict_table)

    dict_tables_values = get_dictionary_values(database_name_ora, dict_tables)

    for variant in variants:
        table, dict_field, dict_table = variant

        values = [row[0] for row in arcpy.da.SearchCursor(table, [dict_field], f"{dict_field} is not null")]
        set_values = set(values)

        records, errors, data = validate_data(dict_tables_values.get(dict_table), set_values)
        if errors:
            print(f'{dict_table} {dict_field} --> {table} id: records: {records}, errors: {errors}')
            file_name = f'{table}__{dict_table}__{dict_field}__{records}__{errors}.log'
            write_log('logMigrateValue', file_name, data)


def get_geometry_wkt(input_file, geom_field, id_field, row_id):
    """ Pobieranie współrzędnych WKT danego rekordu z danego pliku

    :param input_file: plik żródłowy
    :param geom_field: nazwa pola z geometrią
    :param id_field: nazwa pola z uniklanym identyfikatorem
    :param row_id: identyfikator rekordu
    :return: geoemrtia WKT
    """

    with arcpy.da.SearchCursor(input_file, [f'{geom_field}@WKT'], f"{id_field} = '{row_id}'") as cursor:
        for row in cursor:
            return row[0]


def get_geometry_xy_all(input_file, geom_field, row_id):
    """ Pobieranie geometrii X i Y z danej warstwy źródłowej

    :param input_file: plik żródłowy
    :param geom_field: nazwa pola z geometrią
    :param row_id: identyfikator rekordu
    :return: słownik {'identyfikator rekordu': (X, Y), ...}
    """

    geom_tmp = {}
    with arcpy.da.SearchCursor(input_file, [f'{geom_field}@X', f'{geom_field}@Y', row_id]) as cursor:
        for row in cursor:
            if row[2]:
                geom_tmp[row[2]] = (row[0], row[1])

    return geom_tmp


def insert_try(cursor, value):
    """ Funkcja wstawiająca dane

    :param cursor: kursor tabeli do której migrujemy dane
    :param value: lista wartosci, które migryjemy
    :return: informacja o błędzie jeśli wstawianie rekordu zakończyło się niepowodzeniem, lub wartość None
    """

    for i, v in enumerate(value):
        value[i] = v.strip() if type(v) is str else v

    try:

        cursor.insertRow(value)
        err = None

    except Exception as e:
        err = e

    return err
