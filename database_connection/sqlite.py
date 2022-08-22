import sqlite3
import os

PATH_SPATIAL_EXTENSION = r''


class SqliteConn:

    def __init__(self, database_name):
        import sqlite3
        import os
        self.path_extension = PATH_SPATIAL_EXTENSION
        self.path_database = database_name
        self.spatialite_conn = sqlite3.connect(self.path_database)
        self.spatialite_conn.enable_load_extension(True)
        os.environ["PATH"] = self.path_extension + ";" + os.environ["PATH"]
        self.spatialite_conn.load_extension(os.path.join(self.path_extension, "mod_spatialite"))
        self.sqlite3 = sqlite3
        print('---' * 10)
        print('Inicializacja SQliteConn')
        print('---' * 10)

        print(self.sqlite3.version)

    def create_table(self, sql_query):
        cursor = self.spatialite_conn.cursor()
        try:
            cursor.execute(sql_query)
        except Exception as e:
            cursor.close()
            print('Błąd zapytania', e)

    def select_from(self, select_guery):
        if not select_guery.lower().strip().startswith('select'):
            print('Wykonuje tylko select')
            return None
        cursor = self.spatialite_conn.cursor()
        try:
            cursor.execute(select_guery)
        except Exception as e:
            cursor.close()
            self.spatialite_conn.close()
            print('Błąd zapytania', e)
            print('Rozłączono z bazą')
            return None
        values = []
        for row in cursor.fetchall():
            values.append(row)
        cursor.close()
        self.spatialite_conn.close()
        print('Rozłączono z bazą')
        return values

    def insert_into_from_dict(self, class_table, dict_values):
        print(class_table.__tablename__)
        sql = f"""insert into {class_table.__tablename__}"""
        cursor = self.spatialite_conn.cursor()
        for row in dict_values:
            sql_fields = """ ("""
            sql_value = """) values ("""
            for field, value in row.items():
                if value:
                    sql_fields += f"""{field}, """
                    if class_table.__dict__.get(field) == 'geometry':
                        sql_value += f"""GeomFromText('{value}', 2180), """
                    elif class_table.__dict__.get(field) == 'text':
                        sql_value += f"""'{value}', """
                    elif class_table.__dict__.get(field) == 'date':
                        sql_value += f"""'{value.replace(' 00:00:00', '')}', """
                    else:
                        sql_value += f"""{value}, """
            sql_insert = sql + sql_fields[:-2] + sql_value[:-2] + """)"""
            try:
                cursor.execute(sql_insert)
            except Exception as e:
                cursor.close()
                self.spatialite_conn.rollback()
                print(e, sql_insert)
                break
        cursor.close()
        self.spatialite_conn.commit()

    def insert_into(self, class_table, values):
        print(class_table.__tablename__)
        fields_name = [x[0] for x in class_table.__dict__.items() if not x[0].startswith('_')]
        fields_type = [x[1] for x in class_table.__dict__.items() if not x[0].startswith('_')]
        sql = 'insert into ' + class_table.__tablename__
        cursor = self.spatialite_conn.cursor()
        for index, value in enumerate(values):
            sql_fields = ' ('
            sql_value = ') values ('
            for idx, field in enumerate(fields_type):
                if value[idx] is not None:
                    sql_fields += f'{fields_name[idx]}, '
                    if field == 'geometry':
                        sql_value += f"GeomFromText('{value[idx]}', 2180), "
                    elif field == 'text':
                        sql_value += f"'{value[idx]}', "
                    elif field == 'date':  # napisać funkcję walidującą daty
                        sql_value += f"'{value[idx].replace(' 00:00:00', '')}', "
                    else:
                        sql_value += f'{value[idx]}, '
            sql_insert = sql + sql_fields[:-2] + sql_value[:-2] + ')'
            try:
                cursor.execute(sql_insert)
            except Exception as e:
                cursor.close()
                self.spatialite_conn.rollback()
                print(e, 'record:', index, sql_insert)
                break
        cursor.close()
        self.spatialite_conn.commit()

    def update_where(self, sql_query):
        if not sql_query.lower().strip().startswith('update'):
            print('Wykonuje tylko update')
            return None
        cursor = self.spatialite_conn.cursor()
        try:
            cursor.execute(sql_query)
            self.spatialite_conn.commit()
        except Exception as e:
            cursor.close()
            print('Błąd zapytania', e)

    def update_where_script(self, sql_query):
        if not sql_query.lower().strip().startswith('update'):
            print('Wykonuje tylko update')
            return None
        cursor = self.spatialite_conn.cursor()
        try:
            cursor.executescript(sql_query)
            self.spatialite_conn.commit()
        except Exception as e:
            cursor.close()
            print('Błąd zapytania', e)

    def delete_from(self, class_table, where=False):
        if where:
            sql_delete = 'delete from ' + class_table.__tablename__ + ' ' + where
        else:
            sql_delete = 'delete from ' + class_table.__tablename__
        cursor = self.spatialite_conn.cursor()
        try:
            cursor.execute(sql_delete)
            self.spatialite_conn.commit()
        except Exception as e:
            self.spatialite_conn.rollback()
            print(e)

    def vacuum(self):
        sql = 'vacuum'
        cursor = self.spatialite_conn.cursor()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)

    def disconnect(self):
        self.spatialite_conn.close()
