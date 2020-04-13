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

def get_bd_tables():
    file = open('sql/get_all_update_tables.sql', 'r')
    query = file.read();
    file.close();
    res = pd.read_sql(query,cnxn)['name'].tolist()
    res.sort()
    return res

def short_name(name):
    name = name.split(" ")[0]

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
    

def formate_query_currtabnum(tablenames_list, currtabnum, mi):
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
and [машинист_инструктор]="""+mi+"""
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
# @app.route('/')
# def index():    
#     return render_template('layouts/default.html',content=render_template('pages/index.html'), tables_list=get_bd_tables())



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
                                content=render_template( 'pages/'+path), tables_list=get_bd_tables())
    except:
        return render_template('layouts/auth-default.html',
                                content=render_template( 'pages/404.html' ) )


# GET DATABASE MI LIST
@app.route('/getDB', methods=['POST'])
def getDB():    
    # MACHINE MANS
    m_i = request.json['machine_instructor']
    html_dates = json.loads(request.json['date_range'])
    isMipersid = request.json['isMipersid']
    
    if(isMipersid):
        print("MIPERSID "+m_i)
    else:
        print("CURTAB TO MIPERID "+m_i)
        m_i=int(pd.read_sql("""SELECT [mipersid],[currtabnum] FROM [AsuSps].[dbo].[dict] where [currtabnum]="""+m_i,cnxn)["mipersid"])
        print(m_i)

    date_range =[]
    for date in html_dates:
        date_range.append("table_"+datetime.datetime.strptime(date,'%d.%m.%y').strftime('%Y%m%d'))
    print('M_I', m_i, "DATERANGE", date_range)
    query = formate_query_machine(date_range,m_i)
    df = pd.read_sql(query,cnxn)
    df = df.drop_duplicates(df.columns, keep='last')
    df["UID"] = df.LastName.str.cat(" "+df.FirstName).str.cat(" "+df.PatrName).str.cat(" "+df.CurrTabNum.astype(str))
    unique_grouped = df.groupby(['UID'])['ID_SP_NAR'].nunique().reset_index()
    unique_grouped['UID'] = unique_grouped["UID"].apply(lambda row: row.split(" ")[0]+" "+row.split(" ")[1][0]+"."+row.split(" ")[2][0]+". "+row.split(" ")[3])
    
    # GRAPHIC
    graphic_data = [(lambda x: [str(df.loc[df.ID_SP_NAR ==str(144)]["расшифровка"][0])+" "+str(x), df.loc[df['ID_SP_NAR'] == str(x)]["personal_probability"].mean()]   )(x) for x in df.ID_SP_NAR.unique()[:10].astype(int)]
    lables = [(lambda x: str(x[0]))(x) for x in graphic_data]
    data = [(lambda x: float(x[1]))(x) for x in graphic_data]

    graphic_data = json.dumps({ "labels": lables,
                    "datasets": [{
                        "label": '%',
                        "data": data
                    }]
    })

    data = {'message': json.dumps(unique_grouped.values.tolist()), 'tables': json.dumps(date_range), "graphic_data":graphic_data, 'code': 'SUCCESS'}
    return make_response(jsonify(data), 201)

# GET MACHINE MAN
@app.route('/getCurrTabNum', methods=['POST'])
def getCurrTabNum():
    currtab = request.json['currtab'].split(" ")[2]
    tables =  request.json['tables'].split(",")
    mi = request.json['mi']

    query = formate_query_currtabnum(tables, currtab, mi)
    df = pd.read_sql(query,cnxn)
    df = df.drop_duplicates(df.columns, keep='last')
    df['Количество'] = df.groupby('ID_SP_NAR')['ID_SP_NAR'].transform('count')
    df["DATE"]=df['DATE'].apply(lambda row: datetime.datetime.strptime(row.split("_")[1], '%Y%m%d').strftime('%d.%m.%y'))
    uniq = df.groupby(['ID_SP_NAR'])['RoadID'].nunique()
    df['RoadCode'] = pd.Series()
    for idx, row in df.iterrows():
        df['RoadCode'][idx] = uniq[str(row['ID_SP_NAR'])]

    df["NumLokSeries4"] = df["SpsSerieID"].astype(str) +"_"+ df["SpsGroupID"].astype(str)

    df["CountLockSeries"] = df["SpsSerieID"].astype(str) + df["SpsGroupID"].astype(str)
    uniq2 = df.groupby(['ID_SP_NAR'])['CountLockSeries'].nunique()
    for idx, row in df.iterrows():
        df['CountLockSeries'][idx] = str(uniq2[str(row['ID_SP_NAR'])])

    # print(df.columns)
    # print(df.values.tolist()[0])


    data = {'table_data': json.dumps(df.values.tolist()), 'code': 'SUCCESS'}
    return make_response(jsonify(data), 201)

