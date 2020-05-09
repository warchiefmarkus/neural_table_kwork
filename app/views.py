# -*- encoding: utf-8 -*-
import os, logging, datetime, pyodbc, json
import pandas as pd
import numpy as np
from flask               import render_template, request, url_for, jsonify, redirect, send_from_directory, make_response
from werkzeug.exceptions import HTTPException, NotFound, abort
from flask_login         import login_user, logout_user, current_user, login_required

# App modules
from app        import app, lm, db, bc
from app.models import User
from app.forms  import LoginForm, RegisterForm

# provide login manager with load_user callback
@lm.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# Logout user
@app.route('/logout.html')
def logout():
    logout_user()
    return redirect(url_for('index'))

# Register a new user
@app.route('/register.html', methods=['GET', 'POST'])
def register():    
    # declare the Registration Form
    form = RegisterForm(request.form)
    msg = None
    if request.method == 'GET': 
        return render_template('layouts/auth-default.html',
                                content=render_template( 'pages/register.html', form=form, msg=msg ) )

    # check if both http method is POST and form is valid on submit
    if form.validate_on_submit():
        # assign form data to variables
        username = request.form.get('username', '', type=str)
        password = request.form.get('password', '', type=str) 
        email    = request.form.get('email'   , '', type=str) 
        # filter User out of database through username
        user = User.query.filter_by(user=username).first()
        # filter User out of database through username
        user_by_email = User.query.filter_by(email=email).first()
        if user or user_by_email:
            msg = 'Error: User exists!'        
        else:         
            pw_hash = password #bc.generate_password_hash(password)
            user = User(username, email, pw_hash)
            user.save()
            msg = 'User created, please <a href="' + url_for('login') + '">login</a>'   
    else:
        msg = 'Input error'   
    return render_template('layouts/auth-default.html',
                            content=render_template( 'pages/register.html', form=form, msg=msg ) )

# Authenticate user
@app.route('/login.html', methods=['GET', 'POST'])
def login():    
    # Declare the login form
    form = LoginForm(request.form)
    # Flask message injected into the page, in case of any errors
    msg = None

    # check if both http method is POST and form is valid on submit
    if form.validate_on_submit():
        # assign form data to variables
        username = request.form.get('username', '', type=str)
        password = request.form.get('password', '', type=str) 
        # filter User out of database through username
        user = User.query.filter_by(user=username).first()
        if user:            
            #if bc.check_password_hash(user.password, password):
            if user.password == password:
                login_user(user)
                return redirect(url_for('index'))
            else:
                msg = "Wrong password. Please try again."
        else:
            msg = "Unkkown user"

    return render_template('layouts/auth-default.html',
                            content=render_template( 'pages/login.html', form=form, msg=msg ) )


cnxn = pyodbc.connect("Driver={SQL Server};"
                      "Server=asut-db.transinfocom.ru;"
                      "Port=1433;"
                      "UID=AS_NBD_STAT;"
                      "PWD=AS_NBD_STAT654;"
                      "Database=AsuSps;")
cursor = cnxn.cursor()

#-----------------------------------------------------------------------------------
# GET DB TABLES
def get_bd_tables():
    query = "SELECT name FROM AsuSps.sys.Tables WHERE name LIKE '%table_%';"
    res = pd.read_sql(query,cnxn)['name'].tolist()
    res.sort()
    return res

#-----------------------------------------------------------------------------------
def short_name(name):
    name = name.split(" ")[0]

#-----------------------------------------------------------------------------------
# FORMAT QUERY FOR GET MACHINE_MAN_INSTRUCTOR
def formate_query_machine(tablenames_list, machine_id):
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
	  ,[TabNum]
	  ,[personal_probability]
      ,'"""+str(table_name)+"""' AS DATE
      ,[RoadID] as RoadID
FROM [AsuSps].[dbo].["""+str(table_name)+"""]
where [CurrTabNum]="""+str(machine_id)+"""
"""
        if (idx!=len(tablenames_list)-1):
            query+="""
UNION ALL
"""
    return query
    
#-----------------------------------------------------------------------------------
# FORMAT QUERY TO GET MACHINE_MANS OF INSTRUCTORS FROM DB
def formate_query_machine_man(tablenames_list, employer):
    query= ""
    for idx, table_name in enumerate(tablenames_list):
        # and [машинист_инструктор]="""+str(mi)+"""
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
where [TabNum]="""+str(employer)
        if (idx!=len(tablenames_list)-1):
            query+="""
UNION ALL
"""
    return query  

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



@app.template_filter('strftime')
def _jinja2_filter_datetime(date, fmt=None):
    return datetime.datetime.strptime(date, '%Y%m%d').strftime('%d.%m.%y')

# App main route + generic routing
@app.route('/', defaults={'path': 'index.html'})
@app.route('/<path>')
def index(path):
    if not current_user.is_authenticated:
        return redirect(url_for('login'))
    content = None
    try:
        # try to match the pages defined in -> pages/<input file>
        return render_template('layouts/default.html',
                                content=render_template( 'pages/'+path)) #, tables_list=get_bd_tables()
            #   {% for each in tables_list %}
            #   <li class="list-group-item" data="{{ each }}">{{ each | replace("table_", "") | strftime}}</li>
            #   {% endfor %}
    except Exception as e:
        print(e)
        return render_template('layouts/auth-default.html',
                                content=render_template( 'pages/404.html' ) )


#-----------------------------------------------------------------------------------
#1 GET MACHINE_INSTRUCTOR BY CURRTABNUM
@app.route('/getDB', methods=['POST'])
def getDB():    
    currtabnum = int(request.json['machine_instructor'])
    html_dates = request.json['date_range']
    tables_list = get_bd_tables()
    date_range=[]
    from_date = datetime.datetime.strptime(html_dates.split("-")[0],'%m/%Y')
    to_date = datetime.datetime.strptime(html_dates.split("-")[1],'%m/%Y')

    for table in tables_list:
        table_date = datetime.datetime.strptime(datetime.datetime.strptime(table.split("_")[1],'%Y%m%d').strftime('%m/%Y'),'%m/%Y') 
        if(((table_date>=from_date)&(table_date<=to_date))):
            date_range.append(table)
    if(len(date_range)<1):
        data = {'message': {}, 'tables': {}, "graphic_data": {}, 'isEmpty': 'true', 'code': 'SUCCESS'}        
        return make_response(jsonify(data), 201)        

    query = formate_query_machine(date_range, currtabnum)

    df = pd.read_sql(query,cnxn)
    df = df.drop_duplicates(df.columns, keep='last')
    df["UID"] = df.LastName.str.cat(" "+df.FirstName).str.cat(" "+df.PatrName).str.cat(" "+df.TabNum.astype(str))
    unique_grouped = df.groupby(['UID'])['ID_SP_NAR'].nunique().reset_index()
    unique_grouped['UID'] = unique_grouped["UID"].apply(lambda row: row.split(" ")[0]+" "+row.split(" ")[1][0]+"."+row.split(" ")[2][0]+". "+row.split(" ")[3])
    
    # GRAPHIC
    graphic_data = [(lambda x: [str(df.loc[df.ID_SP_NAR ==str(x)]["расшифровка"].values[0])+" "+str(x), df.loc[df.ID_SP_NAR == str(x)]["personal_probability"].mean()]   )(x) for x in df.ID_SP_NAR.unique()[:10].astype(int)]
    lables = [(lambda x: str(x[0]))(x) for x in graphic_data]
    data = [(lambda x: float(x[1]))(x) for x in graphic_data]
    
    graphic_data = json.dumps({ "labels": lables,
                    "datasets": [{
                        "label": '%',
                        "data": data
                    }]
    })

    data = {'message': json.dumps(unique_grouped.values.tolist()), 'tables': json.dumps(date_range), "graphic_data":graphic_data, 'isEmpty': 'false', 'code': 'SUCCESS'}
    return make_response(jsonify(data), 201)

#-----------------------------------------------------------------------------------
# GET MACHINE MAN
@app.route('/getMachineMans', methods=['POST'])
def getMachineMans():

    employer = request.json['currtab'].split(" ")[2]
    tables =  request.json['tables'].split(",")
   
    query = formate_query_machine_man(tables, employer)

    print("Q ", query)
    df = pd.read_sql(query,cnxn)
    df = df.drop_duplicates(df.columns, keep='last')
    df['Количество'] = df.groupby('ID_SP_NAR')['ID_SP_NAR'].transform('count')
    df["DATE"]=df['DATE'].apply(lambda row: datetime.datetime.strptime(row.split("_")[1], '%Y%m%d').strftime('%d.%m.%y'))
    uniq = df.groupby(['ID_SP_NAR'])['RoadID'].nunique()
    df['RoadCode'] = pd.Series()
    for idx, row in df.iterrows():
        df['RoadCode'][idx] = uniq[str(row['ID_SP_NAR'])]

    df["NumLokSeries4"] = df["SpsSerieID"].astype(str) +"_"+ df["SpsGroupID"].astype(str)

    df["CountLockSeries"] = df["SpsSerieID"].astype(str) +"_"+ df["SpsGroupID"].astype(str)+"_"+df["ID_SP_NAR"]
    vc = df["CountLockSeries"].value_counts()
    df["CountLockSeries"] = df["CountLockSeries"].apply(lambda x: vc[x])

    df["RISKS"] = pd.Series()
    for idx, row in df.iterrows():    
        average = (float(pd.read_sql(formate_query_risks(tables,df["ID_SP_NAR"][idx]),cnxn).mean()))
        personal = df["personal_probability"][idx]
        if ((average - personal)/average>=20):
            df['RISKS'][idx] = 2
        elif ((average - personal)/average<20):
            df['RISKS'][idx] = 1

    # print(df.columns)
    # print(df.values.tolist()[0])

    data = {'table_data': json.dumps(df.values.tolist()), 'code': 'SUCCESS'}
    return make_response(jsonify(data), 201)

#-------------------------------------------------------------------------------
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

#-------------------------------------------------------------------------------
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

def formate_query_enterprise_id(tablenames_list, id_sp_nar, enterprise_id):
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

#-----------------------------------------------------------------------------
# GET ROAD_ID SPLIT
@app.route('/getRoadId', methods=['POST'])
def getRoadId():
    road_id = request.json['road_id']
    html_dates =  request.json['daterange']
    tables_list = get_bd_tables()
    date_range=[]
    from_date = datetime.datetime.strptime(html_dates.split("-")[0],'%m/%Y')
    to_date = datetime.datetime.strptime(html_dates.split("-")[1],'%m/%Y')

    for table in tables_list:
        table_date = datetime.datetime.strptime(datetime.datetime.strptime(table.split("_")[1],'%Y%m%d').strftime('%m/%Y'),'%m/%Y') 
        if(((table_date>=from_date)&(table_date<=to_date))):
            date_range.append(table)
    if(len(date_range)<1):
        data = {'message': {}, 'tables': {}, "graphic_data": {}, 'isEmpty': 'true', 'code': 'SUCCESS'}        
        return make_response(jsonify(data), 201)        

    query = formate_query_split(date_range)
    df = pd.read_sql(query,cnxn)
    uniq_road_id=pd.DataFrame(df['ID_SP_NAR'].unique(), columns=["ID_SP_NAR"])
        
    uniq_road_id["AVERAGE"]=pd.Series()
    for idx, row in uniq_road_id.iterrows():    
        uniq_road_id["AVERAGE"][idx] = (str(pd.read_sql(formate_query_road_id(date_range,int(row[0]),road_id),cnxn).mean()))
    uniq_road_id = uniq_road_id.sort_values('AVERAGE', ascending=True)
    data = {'table_data': json.dumps(uniq_road_id.values.tolist()), 'code': 'SUCCESS'}
    return make_response(jsonify(data), 201)

# GET ENTERPRISE_ID SPLIT
@app.route('/getEnterpriseId', methods=['POST'])
def getEnterpriseId():
    enterprise_id = request.json['enterprise_id']
     
    html_dates =  request.json['daterange']
    tables_list = get_bd_tables()
    date_range=[]
    from_date = datetime.datetime.strptime(html_dates.split("-")[0],'%m/%Y')
    to_date = datetime.datetime.strptime(html_dates.split("-")[1],'%m/%Y')

    for table in tables_list:
        table_date = datetime.datetime.strptime(datetime.datetime.strptime(table.split("_")[1],'%Y%m%d').strftime('%m/%Y'),'%m/%Y') 
        if(((table_date>=from_date)&(table_date<=to_date))):
            date_range.append(table)
    if(len(date_range)<1):
        data = {'message': {}, 'tables': {}, "graphic_data": {}, 'isEmpty': 'true', 'code': 'SUCCESS'}        
        return make_response(jsonify(data), 201)        

    query = formate_query_split(date_range)
    df = pd.read_sql(query,cnxn)
    uniq_enterprise_id=pd.DataFrame(df['ID_SP_NAR'].unique(), columns=["ID_SP_NAR"])
    
    uniq_enterprise_id["AVERAGE"]=pd.Series()    
    for idx, row in uniq_enterprise_id.iterrows(): 
        p = pd.read_sql(formate_query_enterprise_id(date_range,int(row[0]),enterprise_id),cnxn).mean() 
        uniq_enterprise_id["AVERAGE"][idx] = str(p)
    uniq_enterprise_id = uniq_enterprise_id.sort_values('AVERAGE', ascending=True)
    data = {'table_data': json.dumps(uniq_enterprise_id.values.tolist()), 'code': 'SUCCESS'}
    return make_response(jsonify(data), 201)