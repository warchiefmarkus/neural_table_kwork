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
SELECT DISTINCT
	 
      asutNbd_violations.IncidentTypeID
      ,asutNbd_violations.IncidentID
      ,asutNbd_violations.ViolationDate as Date_NAR
      ,asutNbd_violations.BrigadeEnterpriseID
      ,asutNbd_violations.ViolationID as ID_SP_NAR,

	  rp.IncidentDate,
      rp.RouteID,
      rp.PersID,
      rp.MIPersID as 'машинист инструктор'
	
      ,rd.[RouteDate]
      ,rd.[RoadID]
      ,rd.[IsApvo]
      ,rd.[RouteTypeID]
      ,rd.[SpsSerieID]
      ,rd.[SpsGroupID]
      ,rd.[KeID]
      ,rd.[EnterpriseID]	
      ,pr5.EnterpriseID

	  ,CASE 
		WHEN DATEDIFF(yyyy, pr5.DateFrom, getdate()) < 1 THEN 'group_0to1y'
		WHEN DATEDIFF(yyyy, pr5.DateFrom, getdate()) >= 1 AND DATEDIFF(yyyy, pr5.DateFrom, getdate()) < 4 THEN 'group_1to3y'
		WHEN DATEDIFF(yyyy, pr5.DateFrom, getdate()) >= 4 AND DATEDIFF(yyyy, pr5.DateFrom, getdate()) < 8 THEN 'group_3to8y'
		WHEN DATEDIFF(yyyy, pr5.DateFrom, getdate()) >= 8 AND DATEDIFF(yyyy, pr5.DateFrom, getdate()) < 12 THEN 'group_8to12y'
		WHEN DATEDIFF(yyyy, pr5.DateFrom, getdate()) >= 12 AND DATEDIFF(yyyy, pr5.DateFrom, getdate()) < 20 THEN 'group_12to20y'
		WHEN DATEDIFF(yyyy, pr5.DateFrom, getdate()) >= 20 THEN 'group_20y'
		ELSE NULL 
	  END AS DateFrom
      
	  ,pr5.DateTo
      ,pr5.TabNum
      ,pr5.IsDetached as 'уволен или нет'

      ,personal.LastName
      ,personal.FirstName
      ,personal.PatrName
      ,personal.CurrEnterpriseID  as KOD_DEPO
      ,personal.CurrDateTo
      ,personal.CurrTabNum
      ,personal.CurrIsDetached

      ,personal.MainTabNum as 'основной персонал это табельники людей'
	  ,rr.[PersRegID]
      ,rr.[RoutePostID]
      
	FROM asutNbd_violations 
		inner JOIN incidents rp ON asutNbd_violations.CasseteID = rp.CasseteID
		inner JOIN  [report_routesIssue] rd on rd.[RouteDate] = rp.IncidentDate
		inner JOIN personal_registration AS PR5 ON rp.PersID = PR5.[PersID]
	    inner JOIN personal ON pr5.[DateTo] = personal.[CurrDateTo]
	    inner JOIN [routes_personal] rr ON pr5.[PersID] = rr.[PersRegID]

	WHERE rp.MIPersID > 1
	and asutNbd_violations.ViolationDate >'2019-07-07 00:00:00.000'
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