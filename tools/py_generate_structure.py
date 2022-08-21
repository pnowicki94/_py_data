"""
Migracja struktur baz danych
"""
import arcpy
import os


def migrate_structure_from_sde_or_gdb_to_gdb(input_database, input_schema, output, tables):
    for i, t in enumerate(tables):
        t = t.split('.')[1] if '.' in t else t

        if t.startswith('V_'):
            print('Pominięto tabele/widok:', t)
            continue

        path_in = input_database + os.sep + input_schema + '.' + t if input_schema else input_database + os.sep + t

        if arcpy.Exists(path_in):
            print(i, t)

            desc = arcpy.Describe(path_in)
            g = desc.shapeType if 'Geometry' in [f.type for f in desc.fields] else None

            if arcpy.Exists(output + os.sep + t):
                print('exists - ' + t)
                continue

            try:
                if g:
                    arcpy.CreateFeatureclass_management(out_path=output, out_name=t, geometry_type=g, template=path_in,
                                                        spatial_reference=path_in)
                else:
                    arcpy.CreateTable_management(out_path=output, out_name=t, template=path_in, config_keyword="")
            except Exception as err:
                print(err)
        else:
            print('Brak tabeli w bazie:', path_in)


if __name__ == '__main__':
    _input = r''
    _input_schema = r''
    _target = r''

    _tables = [
        ''
    ]

    migrate_structure_from_sde_or_gdb_to_gdb(_input, _input_schema, _target, _tables)
