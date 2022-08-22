"""
Skrypt migrujący dane
"""
import arcpy
import os


# def ------------------------------------------------------------------------------------------------------------------
def append_data(_input, _target, table, _user_in=None, _user_out=None):
    if _user_out:
        _target = "{0}/{1}.{2}".format(_target, _user_out, table)
    else:
        _target = "{0}/{1}".format(_target, table)

    if _user_in:
        _input = "{0}/{1}.{2}".format(_input, _user_in, table)
    else:
        _input = "{0}/{1}".format(_input, table)

    try:
        print('try: {0} to {1}'.format(_input, _target))
        arcpy.Append_management(inputs=_input,
                                target=_target,
                                schema_type="TEST"
                                )

        print('append', table)

    except Exception as err:
        print(str(err), table)


def migration_data(path_gdb_in, path_gdb_out, tables=(), table_exceptions=(), user_in=None, user_out=None):
    if not tables:
        arcpy.env.workspace = path_gdb_in
        tb = arcpy.ListTables()
        fc = arcpy.ListFeatureClasses()

        tables = tb + fc

    for tab in tables:

        if tab in table_exceptions:
            continue

        else:
            flag = False
            with arcpy.da.SearchCursor(path_gdb_in + os.sep + tab, ['OBJECTID']) as cur:
                for row in cur:
                    if row:
                        flag = True
                        break
            if flag:
                # print(tab, flag)
                append_data(path_gdb_in, path_gdb_out, tab, user_in, user_out)

            else:

                print('no data', tab)


# params----------------------------------------------------------------------------------------------------------------
gdb_in = r'C:\GisPartner\PN\migracja2021\migracja_BIWALU_test_prod\wawa_szablony_v2_wersja_prod_20220819.gdb'
sde_or_gdb_out = r'C:\GisPartner\PN\KZGWPROD2@SIGWPROD.sde'
username_input = r''
username_output = r'SIGWPROD'
migration_tables = ()
exceptions_tables = ()

if __name__ == '__main__':
    migration_data(gdb_in, sde_or_gdb_out, migration_tables, exceptions_tables, username_input, username_output)
