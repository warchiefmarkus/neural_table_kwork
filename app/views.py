# -*- encoding: utf-8 -*-
import os, logging, datetime, pyodbc, json
# Flask modules
from flask               import render_template, request, url_for, jsonify, redirect, send_from_directory, make_response
from werkzeug.exceptions import HTTPException, NotFound, abort
# App modules
from app        import app, db, bc
from app.models import User
from app.forms  import LoginForm, RegisterForm
import pandas as pd
import numpy as np

cnxn = pyodbc.connect("Driver={SQL Server};"
                      "Server=asut-db.transinfocom.ru;"
                      "Port=1433;"
                      "UID=AS_NBD_STAT;"
                      "PWD=AS_NBD_STAT654;"
                      "Database=AsuSps;")
cursor = cnxn.cursor()

file = open('sql/get_all_update_tables.sql', 'r')
query = file.read();
file.close();

def get_bd_tables():
    res = pd.read_sql(query,cnxn)['name'].tolist()
    res.sort()
    return res

@app.template_filter('strftime')
def _jinja2_filter_datetime(date, fmt=None):
    return datetime.datetime.strptime(date, '%Y%m%d').strftime('%d.%m.%y')
    
# App main route + generic routing
@app.route('/')
def index():    
    return render_template('layouts/default.html',content=render_template('pages/index.html'), tables_list=get_bd_tables())


@app.route('/getDB', methods=['POST'])
def getDB():
    m_i = request.json['machine_instructor']
    date_range = request.json['date_range']
    print('M_I', m_i, "DATERANGE", date_range)


    l = [['max','1'],['den','2'],['mike','3']]
    

    data = {'message': json.dumps(l), 'code': 'SUCCESS'}
    return make_response(jsonify(data), 201)