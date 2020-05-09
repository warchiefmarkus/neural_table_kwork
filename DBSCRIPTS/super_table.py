#encoding:utf-8
import pyodbc, timeit, math
import pandas as pd
import numpy as np
from subprocess import call
#reload(sys)  
#sys.setdefaultencoding('utf-8')

start = timeit.default_timer()
lasttime = start

cnxn = pyodbc.connect("Driver={SQL Server};"
                        "Server=asut-db.transinfocom.ru;"
                        "Port=1433;"
                        "UID=AS_NBD_STAT;"
                        "PWD=AS_NBD_STAT654;"
                        "Database=AsuSps;")

cursor = cnxn.cursor()

query = """
SELECT 
    av.IncidentTypeID,
    av.IncidentID,
    av.ViolationDate as Date_NAR,
    av.ViolationID as ID_SP_NAR,
    rp.PersID,
    rp.MIPersID as 'машинист инструктор',	
	rd.RoadID,
	rd.SpsSerieID,
    rd.SpsGroupID,
    rd.EnterpriseID,     
	CASE 
		WHEN pr5.work_time < 1 THEN 'group_0to1y'
		WHEN pr5.work_time >= 1 AND pr5.work_time < 4 THEN 'group_1to3y'
		WHEN pr5.work_time >= 4 AND pr5.work_time < 8 THEN 'group_3to8y'
		WHEN pr5.work_time >= 8 AND pr5.work_time < 12 THEN 'group_8to12y'
		WHEN pr5.work_time >= 12 AND pr5.work_time < 20 THEN 'group_12to20y'
		WHEN pr5.work_time >= 20 THEN 'group_20y'
		ELSE NULL 
	END AS DateFrom,	
    p.CurrIsDetached as 'fired', -- уволен или нет
    p.MainTabNum as 'TabNum',     -- 'основной персонал это табельники людей'    
    p.LastName,
    p.FirstName,
    p.PatrName,
    p.CurrEnterpriseID as KOD_DEPO,
	p.MainTabNum as 'подчиненные' 
	
	,x.MainTabNum as 'CurrTabNum' ----новое
	,x.LastName as 'фамилия инструктора' ----новое
	,rp.IncidentDate

from incidents rp
join asutNbd_violations av on (av.IncidentID = rp.ID) 
join report_routesIssue rd on (rd.RouteDate = rp.IncidentDate)

join
(
select distinct p.ID, p.LastName, p.MainTabNum
from personal p
) x on (x.ID = rp.MIPersID)
join personal p on (p.ID = rp.PersID)
join
(
select DATEDIFF(yyyy,y.DateFrom, y.DateTo) as work_time, y.PersID
	from
	(
	select pr.PersID, min(pr.DateFrom) as DateFrom,
		max (
		case
			when pr.DateTo is null then pr.DateFrom
			else pr.DateTo
		end) as DateTo
	from personal_registration pr
	group by pr.PersID
	) y
) pr5 on pr5.PersID = p.ID

where 
p.CurrIsDetached = 0 
and av.ViolationDate >'2019-07-07 00:00:00.000'
"""

data = pd.read_sql(query,cnxn)
chunk_size = 50000
chunks_count = math.ceil(data.shape[0]/chunk_size)
for id, df_i in  enumerate(np.array_split(data, chunks_count)):
    df_i.to_csv("output_{id}.csv".format(id=id), index=False, chunksize=chunk_size)

cursor.close()
cnxn.close()
stop = timeit.default_timer()
print('Time: ', stop - start) 

call(["python", "main.py"])