
# -*- coding: utf-8 -*-

import pandas as pd
import arcpy,os
import datetime,time,math
import numpy as np

arcpy.env.overwriteOutput = True

def calculate_time(func):
     
    def inner1(*args, **kwargs):
        begin = time.time()
        func  (*args, **kwargs)
        end = time.time()
        print ('---------------------------')
        print ('Func: ' +  func.__name__)
        print ("Total time: ", round(end - begin))
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
    return series.astype (str)

@calculate_time
def create_ID(df,args):
    if len(args) == 3:
        condi = ((df[args[0]].isnull()) | (df[args[0]] == 0))
        yes   = toStr(df[args[1]]) + '--' + toStr(df[args[2]]) 
        no    = toStr(df[args[1]]) +  '-' + toStr(df[args[0]])  + '-' + toStr(df[args[2]]) 
    if len(args) == 2:
        condi = (df[args[0]].notnull())
        yes   = toStr(df[args[0]]) + '-' + toStr(df[args[1]]) 
        no    = '0' +  '-' + toStr(df[args[0]])
    df['ID']   = np.where(condi,yes,no)

@calculate_time
def create_midPoint(df):

    def shapeToCentoid(value):
        if value:
            return str(round(value.centroid.X,2)) +'-'+ str(round(value.centroid.Y))
        else:
            return None

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
            merge.rename(columns = {nameX:i}, inplace = True)

    merge.drop(merge.filter(regex='_y$').columns, axis=1, inplace=True)

    df      = merge.query('_merge!="both"')
    df_both = merge.query('_merge=="both"')

    condi = (df['_merge']== 'left_only')
    yes   = 'D'
    no    = 'A'
    df['UPDATE_CODE']   = np.where(condi,yes,no)
    df = df.drop(['_merge'],axis = 1)

    return df,df_both

@calculate_time
def Create_Layer_from_df(merge,New_layer):

    print ("Create Later from df")
    print ("total Assets: {}".format(str(merge.shape[0])))

    if not merge.shape[0]:
        return

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
    for i in list_:
        if i[-1]:
            in_rows.insertRow (i[:-1] + [i[-1]])


def createShapeArea(df):

    def shapeToArea(value):
        if value:
            return str(round(value.area))
        else:
            return None

    addCentoidFunc = np.vectorize(shapeToArea)
    df['Area']    = addCentoidFunc(df['SHAPE@'].values)


def createReplic(gush_old,gush_new,New_layer):

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

    df_both = df_both.query('UPDATE_CODE!="delete"')
    df_both = df_both.drop(['_merge'],axis = 1)

    df_both = df_both[~(df_both['Geom_ID_old'].isin(df_both['Geom_ID_new']))]
    
    df_new      = pd.concat([df,df_both])

    df_new['note'] = 'הנתון שטח רשום כאן אינו מהווה אסמכתה. לאסמכתה חוקית לשטח הרשום של חלקה יש לפנות ללשכות המרשם במשרד המשפטים.'
    # df_new = df_new.drop(['Mid_Point','Geom_ID_new','Geom_ID_old'],axis = 1)

    Create_Layer_from_df(df_new,New_layer)

    return df_new


def copyingToRepli(current_gdb,bankal,fgdb_topocad):

    if not arcpy.Exists(current_gdb):
        print ('current not exists')
        current_gdb = bankal
        arcpy.env.workspace = current_gdb
        print (current_gdb)
        polygons_fcs = [i for i in arcpy.ListFeatureClasses("", "POLYGON") if i]
        for i in polygons_fcs:arcpy.management.Copy(i,fgdb_topocad + '\\' + os.path.basename(i)+ '_01')
        for i in polygons_fcs:arcpy.management.Copy(i,fgdb_topocad + '\\' + os.path.basename(i)+ '_02')
        return


    arcpy.env.workspace = current_gdb
    polygons_fcs = [i for i in arcpy.ListFeatureClasses("", "POLYGON")  if i.split('_')[-1] == '02']
    for i in polygons_fcs:arcpy.management.Copy(i,fgdb_topocad + '\\' + os.path.basename(i)[:-2] + '01')

    # # # #   FAKE - only for testing !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! # # # # 
    arcpy.env.workspace = bankal
    polygons_fcs = [i for i in arcpy.ListFeatureClasses("", "POLYGON")] 
    for i in polygons_fcs:arcpy.management.Copy(i,fgdb_topocad + '\\' + os.path.basename(i) + '_02')


    arcpy.env.workspace = bankal
    PointRep = [i for i in arcpy.ListFeatureClasses("", "POINT")  if 'Otentic_Points' in  i.split('_')[0]]
    for i in PointRep:arcpy.management.Copy(i,fgdb_topocad + '\\' + os.path.basename(i) + '_REP')



def checkFieldsForID(check):

    if 'PARCEL_ALL' in check:
        fields = ['GUSH_SUFFIX','GUSH_NUM','PARCEL']
    elif 'SUB_GUSH_ALL_'in check:
        fields = ['GUSH_SUFFIX','GUSH_NUM']
    elif 'TALAR' in check:
        fields = ['TALAR_NUM','TALAR_YEAR']
    elif 'SHEET_K' in check:
        fields = ['GUSH_SUFFIX','GUSH_NUM','SHEET_K_ID']
    elif 'TALAR_TABLE' in check:
        fields = ['TALAR_NUM','TALAR_YEAR','TALAR_ID']
    elif 'GVUL_PSAK_DIN' in check:
        fields = ['GUSH_SUFFIX','PARCEL_','GUSH_NUM']
    elif 'SUB_GUSH_ALL_SHUMA' in check:
        fields = ['GUSH_SUFFIX','GUSH_NUM']
    else:
        return None

    return fields


def copy_current(new_gdb,*args):
    for i in args:
        print (i)
        if 'topocad' in i:
            new_name = new_gdb + '\\'+ os.path.basename(i).split('_')[0] + '.gdb'
            print (new_name)
            if arcpy.Exists(new_name):
                arcpy.Delete_management(new_name)
            arcpy.Copy_management(i,new_name)
        if 'TopoCAD_REP' in i:
            new_name = new_gdb + '\\'+ os.path.basename(i)[:11] + '.gdb'
            print (new_name)
            if arcpy.Exists(new_name):
                arcpy.Delete_management(new_name)
            arcpy.Copy_management(i,new_name)


def createFolder(dic):
    try:
        if not os.path.exists(dic):
            os.makedirs(dic)
    except OSError:
        print ("Error Create dic")
    return dic

def get_out_put_input_folder():
    scriptPath    = os.path.abspath (__file__)
    scriptFolder  = os.path.dirname (scriptPath)
    main_folder   = os.path.dirname (scriptFolder)
    return main_folder



# # # # #  input  # # # # 


bankal = r'C:\Users\Administrator\Desktop\medad\python\Work\replication\Results_Yovav\banakl2.gdb'  #Fake put current

# bankal  = r'C:\Users\Administrator\Desktop\medad\python\Work\replication\Results_Yovav\topocad_14_8_2019.gdb' #Fake put bankal
# bankal  = r'C:\Users\Administrator\Desktop\medad\python\Work\replication\data\data.gdb'

# # # # #     output       # # # # #


main_folder      = get_out_put_input_folder()
result           = main_folder + '\\' + 'Results'
gdb_path_archive = result      + '\\' + 'archive'
gdb_path_current = result      + '\\' + 'current'

current_topocad     = gdb_path_current + '\\' + 'topocad.gdb'
current_TopoCAD_REP = gdb_path_current + '\\' + 'TopoCAD_REP.gdb'

createFolder(result)
createFolder(gdb_path_archive)
createFolder(gdb_path_current)

###### copys 
fgdb_topocad     = gdb_path_archive + '\\' + Get_date(r'topocad') + '.gdb'
fgdb_TopoCAD_REP = gdb_path_archive + '\\' + Get_date(r'TopoCAD_REP') + '.gdb'

##### current


# # # # #     analysis      # # # # #

if not arcpy.Exists(fgdb_topocad):
    Create_GDB(fgdb_topocad)

if not arcpy.Exists(fgdb_TopoCAD_REP):
    Create_GDB(fgdb_TopoCAD_REP)

copyingToRepli(current_topocad,bankal,fgdb_topocad)

arcpy.env.workspace = fgdb_topocad
polygons_fcs = list(set([i[:-2] for i in arcpy.ListFeatureClasses()]))
layers = [[fgdb_topocad + '\\' + i+'01',fgdb_topocad + '\\' + i+'02'] for i in polygons_fcs]

print (layers)

for i in layers:

    fields = checkFieldsForID(i[0])
    if not fields:
        print ('layer: {}, dosent have fields yet'.format(i))
        continue

    layer_old = i[0]
    layer_new = i[1]

    print (layer_old)
    print (layer_new)

    layername = fgdb_TopoCAD_REP + '\\' + os.path.basename(layer_new)[:-3]

    df_old = Read_Fc(layer_old)
    df_new = Read_Fc(layer_new)

    create_ID(df_old,fields) 
    create_ID(df_new,fields) 

    createReplic(df_old,df_new,layername)

copy_current(gdb_path_current,fgdb_topocad,fgdb_TopoCAD_REP)

