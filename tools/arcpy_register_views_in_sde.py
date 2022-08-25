# -*- coding: utf-8 -*-
"""
Rejestracja widoków bazodanowych w SDE
"""
import os
import arcpy


def register_views(_views, _path_sde, _user):
    for view, geometryType in _views.items():
        print(view)
        try:
            arcpy.RegisterWithGeodatabase_management(in_dataset=_path_sde + os.sep + _user + '.' + view,
                                                     in_object_id_field="OBJECTID",
                                                     in_shape_field="SHAPE",
                                                     in_geometry_type=geometryType,
                                                     in_spatial_reference="PROJCS['ETRS_1989_Poland_CS92',GEOGCS['GCS_ETRS_1989',DATUM['D_ETRS_1989',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Transverse_Mercator'],PARAMETER['False_Easting',500000.0],PARAMETER['False_Northing',-5300000.0],PARAMETER['Central_Meridian',19.0],PARAMETER['Scale_Factor',0.9993],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]];-5119200 -15295100 10000;-100000 10000;-100000 10000;0,001;0,001;0,001;IsHighPrecision",
                                                     in_extent="")
        except Exception as e:
            print(e)


# database path, user --------------------------------------------------------------------------------------------------
paths_sde = {'': (r'', ''),

}

# database -------------------------------------------------------------------------------------------------------------
database = ''
# ----------------------------------------------------------------------------------------------------------------------
path_sde, user = paths_sde[database]

# key: view name, value: geometry type 
views = {
    '': 'Point',
    '': 'Polygon'
}

if __name__ == '__main__':
    register_views(views, path_sde, user)
