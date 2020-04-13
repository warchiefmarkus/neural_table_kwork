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

df["NumLokSeries4"] = df["SpsSerieID"].astype(str) + df["SpsGroupID"].astype(str)
uniq2 = df.groupby(['ID_SP_NAR'])['NumLokSeries4'].nunique()
for idx, row in df.iterrows():
    df['NumLokSeries4'][idx] = uniq2[str(row['ID_SP_NAR'])]



