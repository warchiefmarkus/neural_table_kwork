# -*- coding: utf-8 -*-
"""
Редактор Spyder
Это временный скриптовый файл.
"""

import pyodbc, json
import pandas as pd
import numpy as np

cnxn = pyodbc.connect("Driver={SQL Server};"
                      "Server=asut-db.transinfocom.ru;"
                      "Port=1433;"
                      "UID=AS_NBD_STAT;"
                      "PWD=AS_NBD_STAT654;"
                      "Database=AsuSps;")
cursor = cnxn.cursor()


def short_name(name):
    name = name.split(" ")[0]

def formate_query(tablenames_list, machine_id):
    query= ""
    for idx, table_name in enumerate(tablenames_list):
        print(table_name)
        query +="""
SELECT DISTINCT 
       [машинист_инструктор]
      ,[LastName]
      ,[FirstName] 
      ,[PatrName]
      ,[ID_SP_NAR]	
	  ,[расшифровка]
	  ,[KOD_DEPO]
      ,[DateFrom] as стаж
      ,[SpsSerieID]
	  ,[SpsGroupID]
	  ,[CurrTabNum]
	  ,[personal_probability]
      ,'"""+str(table_name)+"""' AS DATE
      ,[RoadID] as RoadID
FROM [AsuSps].[dbo].["""+str(table_name)+"""]
where [машинист_инструктор]="""+str(machine_id)+"""
"""
        if (idx!=len(tablenames_list)-1):
            query+="""
UNION ALL
"""
    return query
    
query = formate_query(["table_20200324","table_20200323"],12488)


df = pd.read_sql(query,cnxn)
df = df.drop_duplicates(df.columns, keep='last')

df.query('ID_SP_NAR == 144').head(1)
df[df['ID_SP_NAR'] == 144].head(2)


graphic_data = [(lambda x: [str(df.loc[df.ID_SP_NAR ==str(144)]["расшифровка"][0])+" "+str(x), df.loc[df['ID_SP_NAR'] == str(x)]["personal_probability"].mean()]   )(x) for x in df.ID_SP_NAR.unique()[:10].astype(int)]
lables = [(lambda x: str(x[0]))(x) for x in graphic_data]
data = [(lambda x: float(x[1]))(x) for x in graphic_data]

a = json.dumps({ "labels": lables,
				"datasets": [{
					"label": '%',
					"data": data
				}]
})



df["UID"] = df.LastName.str.cat(" "+df.FirstName).str.cat(" "+df.PatrName).str.cat(" "+df.CurrTabNum.astype(str))

unique_grouped = df.groupby(['UID'])['ID_SP_NAR'].nunique().reset_index()
unique_grouped['UID'] = unique_grouped["UID"].apply(lambda row: row.split(" ")[0]+" "+row.split(" ")[1][0]+"."+row.split(" ")[2][0]+". "+" ("+row.split(" ")[3]+")")


amm = df.groupby(['UID','ID_SP_NAR']).size()


#------------------------------------------------------------------------------

currtabnum = "11349946"
tablenames_list =  ["table_20200324","table_20200323"]
query= ""

for idx, table_name in enumerate(tablenames_list):
        print(table_name)
        query +=""" 
SELECT DISTINCT 
       [LastName]
      ,[FirstName] 
      ,[PatrName]
      ,[ID_SP_NAR]	
	  ,[расшифровка]
	  ,[KOD_DEPO]
      ,[DateFrom] as стаж
      ,[SpsSerieID]
	  ,[SpsGroupID]
	  ,[CurrTabNum]
	  ,[personal_probability]
      ,'"""+str(table_name)+"""' AS DATE
      ,[RoadID] as RoadID
FROM [AsuSps].[dbo].["""+str(table_name)+"""]
where [CurrTabNum]="""+str(currtabnum)+"""
and [машинист_инструктор]=12488
"""
        if (idx!=len(tablenames_list)-1):
            query+="""
UNION ALL
"""

import json, datetime

df = pd.read_sql(query,cnxn)
df = df.drop_duplicates(df.columns, keep='last')

df['Количество'] = df.groupby('ID_SP_NAR')['ID_SP_NAR'].transform('count')

uniq = df.groupby(['ID_SP_NAR'])['RoadID'].nunique()
df['RoadCode'] = pd.Series()

for idx, row in df.iterrows():
    df['RoadCode'][idx] = uniq[str(row['ID_SP_NAR'])]

df["DATE"]=df['DATE'].apply(lambda row: datetime.datetime.strptime(row.split("_")[1], '%Y%m%d').strftime('%d.%m.%y'))




df["CountLockSeries"] = df["SpsSerieID"].astype(str) +"_"+ df["SpsGroupID"].astype(str)+"_"+df["ID_SP_NAR"]
vc = df["CountLockSeries"].value_counts()
df["CountLockSeries"] = df["CountLockSeries"].apply(lambda x: vc[x])

    
uniq2 = df.groupby(['ID_SP_NAR'])['CountLockSeries'].nunique() # 19 уникальных склеек спс + спс гроуп
# теперь для каждой строчки в выгрузке ДФ
for idx, row in df.iterrows():
    df['CountLockSeries'][idx] = str(uniq2[str(row['ID_SP_NAR'])]) # колонка КаунтЛокСериес от индекса
    # равна 





mi = 92380685
mi=int(pd.read_sql("""SELECT [mipersid],[currtabnum] FROM [AsuSps].[dbo].[dict] where [currtabnum]="""+str(mi),cnxn)["mipersid"])

#------------------------------------------------------------------------------


def formate_query_risks(tablenames_list, id_sp_nar):
    query= ""
    for idx, table_name in enumerate(tablenames_list):
        query +="""
select avg([personal_probability])
FROM [AsuSps].[dbo].["""+str(table_name)+"""]
where [ID_SP_NAR]="""+str(id_sp_nar)+"""
"""
        if (idx!=len(tablenames_list)-1):
            query+="""
UNION ALL
"""
    return query


tablenames_list =  ["table_20200324","table_20200323"]
df["RISKS"] = pd.Series()

for idx, row in df.iterrows():    
    average = (float(pd.read_sql(formate_query_risks(tablenames_list,df["ID_SP_NAR"][idx]),cnxn).mean()))
    personal = df["personal_probability"][idx]
    if ((average - personal)/average>=20):
        df['RISKS'][idx] = 2
    elif ((average - personal)/average<20):
        df['RISKS'][idx] = 1
    




m=pd.read_sql(formate_query_risks(tablenames_list,df["ID_SP_NAR"][idx]),cnxn)
    
    
#--------------------------------------------------
tablenames_list =  ["table_20200324","table_20200323"]
road_id = 96


def formate_query_split(tablenames_list):
    query= ""
    for idx, table_name in enumerate(tablenames_list):
        query +="""
select ID_SP_NAR, RoadID
FROM [AsuSps].[dbo].["""+str(table_name)+"""]"""

        if (idx!=len(tablenames_list)-1):
            query+="""
UNION ALL
"""
    return query

def formate_query_road_id(tablenames_list, id_sp_nar, road_id):
    query= ""
    for idx, table_name in enumerate(tablenames_list):
        query +="""
select
avg ([personal_probability])
FROM [AsuSps].[dbo].["""+str(table_name)+"""]
    where [ID_SP_NAR]="""+str(id_sp_nar)+""" 
    and [RoadID]="""+str(road_id)+""" 
group by [RoadID]
"""
        if (idx!=len(tablenames_list)-1):
            query+="""
UNION ALL
"""
    return query

query = formate_query_split(tablenames_list)
df = pd.read_sql(query,cnxn)
uniq_road_id=pd.DataFrame(df['ID_SP_NAR'].unique(), columns=["ID_SP_NAR"])
    
#--road_id
uniq_road_id["AVERAGE"]=pd.Series()
for idx, row in uniq_road_id.iterrows():    
    uniq_road_id["AVERAGE"][idx] = (float(pd.read_sql(formate_query_road_id(tablenames_list,int(row[0]),road_id),cnxn).mean()))
    
    
#--------------------------------------------------
tablenames_list =  ["table_20200324","table_20200323"]
enterprise_id = 3629


def formate_query_split(tablenames_list):
    query= ""
    for idx, table_name in enumerate(tablenames_list):
        query +="""
select ID_SP_NAR, EnterpriseID
FROM [AsuSps].[dbo].["""+str(table_name)+"""]"""

        if (idx!=len(tablenames_list)-1):
            query+="""
UNION ALL
"""
    return query

def formate_query_road_id(tablenames_list, id_sp_nar, road_id):
    query= ""
    for idx, table_name in enumerate(tablenames_list):
        query +="""
select
avg ([personal_probability])
FROM [AsuSps].[dbo].["""+str(table_name)+"""]
    where [ID_SP_NAR]="""+str(id_sp_nar)+""" 
    and [EnterpriseID]="""+str(enterprise_id)+""" 
group by [EnterpriseID]
"""
        if (idx!=len(tablenames_list)-1):
            query+="""
UNION ALL
"""
    return query

query = formate_query_split(tablenames_list)
df = pd.read_sql(query,cnxn)
uniq_enterprise_id=pd.DataFrame(df['ID_SP_NAR'].unique(), columns=["ID_SP_NAR"])
    
#--road_id
uniq_enterprise_id["AVERAGE"]=pd.Series()
for idx, row in uniq_enterprise_id.iterrows(): 
    p= pd.read_sql(formate_query_road_id(tablenames_list,int(row[0]),enterprise_id),cnxn).mean() 
    print(p)
    uniq_enterprise_id["AVERAGE"][idx] = p
    
print(uniq_enterprise_id.values.tolist())
    

["mipersid"]
AA=pd.read_sql("""SELECT TOP(1) [машинист_инструктор],[CurrTabNum] FROM [AsuSps].[dbo].[table_20200329] where [CurrTabNum]=51439390""",cnxn)

    
    
1) разделение вверх 

2)	период должен выбираться обычным способом: С (число) по (число). Сейчас я вообще не понял, что выбирается. Сутки?
тут надо сделать  просто типа календарника

3) currtabnum -> он теперь называется x.MainTabNum as 'табельный номер инструктора' 
   'основной персонал это табельники людей' для подчинныех
 
4)	Нужен вывод в эксель всех таблиц для печати.
    
    
    
    
    
