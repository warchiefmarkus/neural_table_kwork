# -*- coding: utf-8 -*-
import pyodbc, time, glob
import pandas as pd

cnxn = pyodbc.connect("Driver={SQL Server};"
                "Server=asut-db.transinfocom.ru;"
                "Port=1433;"
                "UID=AS_NBD_STAT;"
                "PWD=AS_NBD_STAT654;"
                "Database=AsuSps;", autocommit = True)

cur = cnxn.cursor()

table_name = "tableDEBUG_"+time.strftime("%Y%m%d", time.localtime())

# CREATE TABLE
create_table_sql = '''
CREATE TABLE {0}(
ID_SP_NAR varchar(20),
PersID integer,
машинист_инструктор integer,
RoadID integer,
SpsSerieID integer,
SpsGroupID integer,
EnterpriseID integer,
DateFrom varchar(20),
fired bit,
TabNum integer,
LastName varchar(20),
FirstName varchar(20),
PatrName varchar(20),
KOD_DEPO integer,
CurrTabNum integer,
personal_probability float,
general_probability float,
расшифровка varchar(200)
)
'''.format(table_name)
cur.execute(create_table_sql)


# EXPORT CSV TO DB
input_list = glob.glob("update_*.csv")

#input_list = input_list[:1] # DEBUG
for file in input_list:
    print(file)
    PATH=file    
    df = pd.read_csv(file, sep=',', encoding='utf-8')
    for index, row in df.iterrows():
        insert_row_sql = '''
        INSERT INTO {0}(ID_SP_NAR, PersID, машинист_инструктор, RoadID, SpsSerieID, SpsGroupID, EnterpriseID, DateFrom, fired, TabNum, LastName, FirstName, PatrName, KOD_DEPO, CurrTabNum, personal_probability, general_probability, расшифровка) 
        values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        '''.format(table_name)
        
#        print(index)
        try:
            cur.execute(insert_row_sql, 
                               str(row[0]),
                               int(row[1]),
                               int(row[2]),
                               int(row[3]),
                               int(row[4]),
                               int(row[5]),
                               int(row[6]),
                               str(row[7]),
                               int(row[8]),
                               int(row[9]),
                               str(row[10]),
                               str(row[11]),
                               str(row[12]),
                               int(row[13]),
                               int(row[14]),
                               float(row[15]),
                               float(row[16]),
                               str(row[17]))
        except Exception as e:
            print(index, e)
               
cur.close()
cnxn.close()















