# -*- coding: utf-8 -*-
import arcpy


def truncate_gdb(gdb, exceptions=None):
    arcpy.env.workspace = gdb
    tb = arcpy.ListTables()
    fc = arcpy.ListFeatureClasses()
    for table in tb + fc:
        if exceptions and table in exceptions:
            continue
        arcpy.TruncateTable_management(in_table=table)
        print("'{0}',".format(table))


if __name__ == '__main__':
    gdb_ = r''
    truncate_gdb(gdb_)
