# -*- encoding: utf-8 -*-
import os, logging, datetime, pyodbc, json
import pandas as pd
import numpy as np
from flask               import render_template, request, url_for, jsonify, redirect, send_from_directory, make_response
from werkzeug.exceptions import HTTPException, NotFound, abort
from app        import app

cnxn = pyodbc.connect("Driver={SQL Server};"
                      "Server=asut-db.transinfocom.ru;"
                      "Port=1433;"
                      "UID=AS_NBD_STAT;"
                      "PWD=AS_NBD_STAT654;"
                      "Database=AsuSps;")
cursor = cnxn.cursor()

def get_bd_tables():
    file = open('sql/get_all_update_tables.sql', 'r')
    query = file.read();
    file.close();
    res = pd.read_sql(query,cnxn)['name'].tolist()
    res.sort()
    return res

def short_name(name):
    name = name.split(" ")[0]

def formate_query(tablenames_list, machine_id):
    query= ""
    for idx, table_name in enumerate(tablenames_list):
        print(table_name)
        query +="""
SELECT [ID_SP_NAR]    
,[машинист_инструктор]
,[LastName]
,[FirstName]
,[PatrName]
,[KOD_DEPO]
,[CurrTabNum]
,[personal_probability]
,[расшифровка]
FROM [AsuSps].[dbo].["""+str(table_name)+"""]
where [машинист_инструктор]="""+str(machine_id)+"""
"""
        if (idx!=len(tablenames_list)-1):
            query+="""
UNION ALL
"""
    return query   

def formate_query_currtabnum(tablenames_list, currtabnum):
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
FROM [AsuSps].[dbo].["""+str(table_name)+"""]
where [CurrTabNum]="""+str(currtabnum)+"""
"""
        if (idx!=len(tablenames_list)-1):
            query+="""
UNION ALL
"""
    return query  


@app.template_filter('strftime')
def _jinja2_filter_datetime(date, fmt=None):
    return datetime.datetime.strptime(date, '%Y%m%d').strftime('%d.%m.%y')
    
# App main route + generic routing
@app.route('/')
def index():    
    return render_template('layouts/default.html',content=render_template('pages/index.html'), tables_list=get_bd_tables())

# GET DATABASE MI LIST
@app.route('/getDB', methods=['POST'])
def getDB():
    m_i = request.json['machine_instructor']
    html_dates = json.loads(request.json['date_range'])
    date_range =[]
    for date in html_dates:
        date_range.append("table_"+datetime.datetime.strptime(date,'%d.%m.%y').strftime('%Y%m%d'))
    print('M_I', m_i, "DATERANGE", date_range)
    query = formate_query(date_range,m_i)
    df = pd.read_sql(query,cnxn)
    df["UID"] = df.LastName.str.cat(" "+df.FirstName).str.cat(" "+df.PatrName).str.cat(" "+df.CurrTabNum.astype(str))
    unique_grouped = df.groupby(['UID'])['ID_SP_NAR'].nunique().reset_index()
    unique_grouped['UID'] = unique_grouped["UID"].apply(lambda row: row.split(" ")[0]+" "+row.split(" ")[1][0]+"."+row.split(" ")[2][0]+". "+row.split(" ")[3])
    
    data = {'message': json.dumps(unique_grouped.values.tolist()), 'tables': json.dumps(date_range), 'code': 'SUCCESS'}
    return make_response(jsonify(data), 201)

# GET MI 
@app.route('/getCurrTabNum', methods=['POST'])
def getCurrTabNum():
    currtab = request.json['currtab'].split(" ")[2]
    tables =  request.json['tables'].split(",")
    print(currtab, " TABLES ", tables)
    query = formate_query_currtabnum(tables, currtab)
    df = pd.read_sql(query,cnxn)
    df = df.drop_duplicates(df.columns, keep='last')
    df['Количество'] = df.groupby('ID_SP_NAR')['ID_SP_NAR'].transform('count')

    data = {'message': json.dumps(df[:10].values.tolist()), 'code': 'SUCCESS'}
    return make_response(jsonify(data), 201)