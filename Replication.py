
# -*- coding: utf-8 -*-

import pandas as pd
import arcpy,os
import datetime
import numpy as np


def Read_Fc(addr,num_rows = 9999999):

    print ("read: Read Fc")
    columns = [f.name for f in arcpy.ListFields(addr) if f.name not in ('SHAPE')] + ['SHAPE@WKT']
    df       = pd.DataFrame(data = [row for row in arcpy.da.SearchCursor\
               (addr,columns,"\"OBJECTID\" < {}".format(num_rows))],columns = columns)
    
    return df


def toStr(series):
    return series.astype (int).astype (str)


def create_ID(df):
    condi = ((df['GUSH_SUFFIX'].isnull()) | (df['GUSH_SUFFIX'] == 0))
    yes   = toStr(df['GUSH_NUM']) + '--' + toStr(df['PARCEL']) 
    no    = toStr(df['GUSH_NUM']) +  '-' + toStr(df['GUSH_SUFFIX'])  + '-' + toStr(df['PARCEL']) 
    df['ID']   = np.where(condi,yes,no)


def create_midPoint(df):
    def reduce_accuracy(tuple_):
        return str(round(tuple_[0],2)) +'-'+ str(round(tuple_[1],2))
    addFun2 = np.vectorize(reduce_accuracy)
    df['Mid_Point'] = addFun2(df['Shape'].values)


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


def find_AppendDelete(df_old,df_new,id_field):
    df = pd.merge(df_old, df_new, on = id_field, how="outer", indicator=True, suffixes=('', '_y')).query('_merge!="both"')
    df.drop(df.filter(regex='_y$').columns, axis=1, inplace=True)

    condi = (df['_merge']== 'left_only')
    yes   = 'A'
    no    = 'D'
    df['UPDATE_CODE']   = np.where(condi,yes,no)
    df = df.drop(['_merge'],axis = 1)

    return df



# # # # #     input       # # # # #
gush_exists  = r'C:\Users\Administrator\Desktop\medad\python\Work\replication\data\data.gdb\PARCEL_ALL'
gush_new     = r'C:\Users\Administrator\Desktop\medad\python\Work\replication\data\input.gdb\PARCEL_ALL'

# # # # #     output       # # # # #
gdb_path     = r'C:\Users\Administrator\Desktop\medad\python\Work\replication\Results'
fgdb_topocad = gdb_path + '\\' + Get_date(r'topocad_X')

# # # # #     analysis      # # # # #

# if arcpy.Exists(fgdb_topocad):
#     arcpy.Delete_management(fgdb_topocad)
# Create_GDB(fgdb_topocad)

gush_old = Read_Fc(gush_exists)
gush_new = Read_Fc(gush_new)

create_ID(gush_old)
create_ID(gush_new)

create_midPoint(gush_old)
create_midPoint(gush_new)

df = find_AppendDelete(gush_old,gush_new,'ID')
