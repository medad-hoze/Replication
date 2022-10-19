
# -*- coding: utf-8 -*-

import pandas as pd
import arcpy,os
import datetime,time,math
import numpy as np

arcpy.env.overwriteOutput = True

def calculate_time(func):
     
    def inner1(*args, **kwargs):
        begin = time.time()
        func(*args, **kwargs)
        end = time.time()
        print ('---------------------------')
        print ('Func: ' +  func.__name__)
        print("Total time: ", end - begin)
        print ('---------------------------')
 
    return inner1


def Read_Fc(addr,num_rows = 9999999):

    print ("read: Read Fc")
    # columns = [f.name for f in arcpy.ListFields(addr) if f.name not in ('SHAPE')] + ['SHAPE@WKT']
    columns = [f.name for f in arcpy.ListFields(addr) if f.name not in ('SHAPE')] + ['SHAPE@']
    df       = pd.DataFrame(data = [row for row in arcpy.da.SearchCursor\
               (addr,columns,"\"OBJECTID\" < {}".format(num_rows))],columns = columns)
    return df


def toStr(series):
    return series.astype (int).astype (str)

@calculate_time
def create_ID(df):
    condi = ((df['GUSH_SUFFIX'].isnull()) | (df['GUSH_SUFFIX'] == 0))
    yes   = toStr(df['GUSH_NUM']) + '--' + toStr(df['PARCEL']) 
    no    = toStr(df['GUSH_NUM']) +  '-' + toStr(df['GUSH_SUFFIX'])  + '-' + toStr(df['PARCEL']) 
    df['ID']   = np.where(condi,yes,no)

@calculate_time
def create_midPoint(df):

    def shapeToCentoid(value):
        return str(round(value.centroid.X,2)) +'-'+ str(round(value.centroid.Y))

    addCentoidFunc = np.vectorize(shapeToCentoid)
    df['Mid_Point']    = addCentoidFunc(df['SHAPE@'].values)

@calculate_time
def Create_GDB(fgdb_name):
    GDB_file = os.path.dirname(fgdb_name)
    GDB_name = os.path.basename(fgdb_name)
    if os.path.exists(fgdb_name):
        return fgdb_name

    print (fgdb_name)
    fgdb_name = str(arcpy.CreateFileGDB_management(GDB_file, GDB_name, "CURRENT"))
    return fgdb_name


def Get_date(name):
    now = datetime.datetime.now()
    return name +'_' + str(now.day) +'_'+ str(now.month) + '_' + str(now.year)


def moveIfNull(df,fieldCheck,fieldMove):
    y = np.where(df[fieldCheck].isnull().values,
                df[fieldMove].values,
                df[fieldCheck].values)

    df[fieldCheck] = y

def find_AppendDelete(df_old,df_new,id_field):

    merge   = pd.merge(df_old, df_new, on = id_field, how="outer", indicator=True, suffixes=('__x', '__y'))
    columns_names = list(set([i.split('__x')[0] for i in merge.columns]))
    for i in columns_names:
        nameX = i + '__x'
        nameY = i + '__y'
        if nameX in merge.columns:
            moveIfNull(merge,nameX,nameY)
            print(merge[[nameX,nameY]])
            merge.rename(columns = {nameX:i}, inplace = True)

    merge.drop(merge.filter(regex='_y$').columns, axis=1, inplace=True)

    df      = merge.query('_merge!="both"')
    df_both = merge.query('_merge=="both"')

    condi = (df['_merge']== 'left_only')
    yes   = 'A'
    no    = 'D'
    df['UPDATE_CODE']   = np.where(condi,yes,no)
    df = df.drop(['_merge'],axis = 1)

    return df,df_both

@calculate_time
def Create_Layer_from_df(merge,New_layer):

    print ("Create Later from df")
    print ("total Assets: {}".format(str(merge.shape[0])))

    for i in ['OBJECTID','Shape']:
        if i in merge.columns: 
            merge = merge.drop([i],axis = 1)

    columns          = list(merge.columns)

    list_            = merge.values.tolist()
    gdb_proc,fc_name = os.path.split(New_layer)
    Fc_rimon         = arcpy.CreateFeatureclass_management (gdb_proc, fc_name, 'POLYGON')

    dict_types = {'int64':'LONG','object':'TEXT','float64':'DOUBLE',\
                  'int32':'SHORT','<M8[ns]':'DATE','datetime64[ns]':'DATE'}

    dict_col_type = {col_name:dict_types[str(type_)] for col_name, type_ in merge.dtypes.to_dict().items()}

    for i in columns:arcpy.AddField_management(Fc_rimon,i,dict_col_type[i])

    columns = columns
    in_rows = arcpy.da.InsertCursor(Fc_rimon,columns)

    print (len(columns))
    print (len(list_[0]))
    for i in list_:in_rows.insertRow (i[:-1] + [i[-1]])


def createShapeArea(df):

    def shapeToArea(value):
        return str(round(value.area,2))

    addCentoidFunc = np.vectorize(shapeToArea)
    df['Area']    = addCentoidFunc(df['SHAPE@'].values)


# # # # #     input       # # # # #
gush_exists  = r'C:\Users\Administrator\Desktop\medad\python\Work\replication\data\data.gdb\PARCEL_ALL'
gush_new     = r'C:\Users\Administrator\Desktop\medad\python\Work\replication\data\input.gdb\PARCEL_ALL'

# # # #    big input    # # #

gush_exists = r'C:\Users\Administrator\Desktop\medad\python\Work\replication\Results_Yovav\topocad_14_8_2019.gdb\PARCEL_ALL_01'
gush_new    = r'C:\Users\Administrator\Desktop\medad\python\Work\replication\Results_Yovav\topocad_14_8_2019.gdb\PARCEL_ALL_02'

# # # # #     output       # # # # #
gdb_path     = r'C:\Users\Administrator\Desktop\medad\python\Work\replication\Results'
fgdb_topocad = gdb_path + '\\' + Get_date(r'topocad') + '.gdb'


New_layer   = fgdb_topocad + '\\' + 'PARCEL_ALL'

# # # # #     analysis      # # # # #

if arcpy.Exists(fgdb_topocad):
    arcpy.Delete_management(fgdb_topocad)
Create_GDB(fgdb_topocad)

gush_old = Read_Fc(gush_exists)
gush_new = Read_Fc(gush_new)

create_ID(gush_old)
create_ID(gush_new)

create_midPoint(gush_old)
create_midPoint(gush_new)

createShapeArea(gush_old)
createShapeArea(gush_new)

gush_old['Geom_ID_old'] = gush_old['Mid_Point'].astype (str) +'-'+ gush_old['Area']
gush_new['Geom_ID_new'] = gush_new['Mid_Point'].astype (str) +'-'+ gush_new['Area']

df,df_both = find_AppendDelete(gush_old,gush_new,'ID')

condi    = (df_both['Geom_ID_old'] == df_both['Geom_ID_new'] )
yes      = 'delete' 
no       = 'U'
df_both['UPDATE_CODE'] = np.where(condi,yes,no)
df_both   = df_both.query('UPDATE_CODE!="delete"')
df_Update = df_both.drop(['_merge'],axis = 1)

df_new      = pd.concat([df,df_Update])


Create_Layer_from_df(df_new,New_layer)



