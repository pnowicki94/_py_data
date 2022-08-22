# -*- coding: utf-8 -*-
import arcpy


# truncate gdb tables
def truncate_gdb(gdb, exceptions=None):
    arcpy.env.workspace = gdb
    tb = arcpy.ListTables()
    fc = arcpy.ListFeatureClasses()
    for table in tb + fc:
        if exceptions and table in exceptions:
            continue
        print('truncate: ', gdb.split('\\')[-1], table)
        arcpy.TruncateTable_management(in_table=table)
