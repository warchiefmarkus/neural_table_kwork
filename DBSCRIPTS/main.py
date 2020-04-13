#encoding:utf-8
import timeit, glob
from generation_data import generation_data
from subprocess import call


start = timeit.default_timer()
lasttime = start

input_list = glob.glob("output_*.csv") # get all output.csv from curent folder

#input_list = input_list[:1] #DEBUG
for file in input_list:
    print(file)
    PATH=file    
    path_output_data=PATH.split(".")[0].replace("output","update")+".csv"

    path_rb = 'data/rb.csv'
    keys = ['ID_SP_NAR',
            'PersID',
            'машинист_инструктор',
            'RoadID',
            'SpsSerieID',
            'SpsGroupID',
            'EnterpriseID',
            'DateFrom',
            'fired',
            'TabNum',            
            'LastName',
            'FirstName',
            'PatrName',
            'KOD_DEPO',
            'CurrTabNum',
            'personal_probability',
            'general_probability',
            'расшифровка'
            ]
    
    data = generation_data(path_from=PATH, path_to=path_output_data, rb=path_rb)
    d=data.generation_csv_table()
    
    d_sort = data.sorted_personal(d) #DEBUG  # сортировка по табельнику
    probability = data.table_probability(d_sort)
    d_sort.append(probability)
    probability_general = data.probability_general(d_sort)    
    d_sort.append(probability_general)    
    crypt = data.tab_nar(d_sort[0])    
    d_sort.append(crypt)    
    
#    d_sort_cache = d_sort # DEBUG
#    d_sort = d_sort_cache # DEBUG
    
    d_sort = data.table_sorted(keys, d_sort)
    
    #FIX
    temp = d_sort['fired']
    d_sort['fired']=d_sort['TabNum']    
    d_sort['TabNum']=temp
    
    data.cvs_create(d_sort)
    stop = timeit.default_timer()
    print('Time: ', stop - start)

call(["python", "export_all_today_table.py"])

    
    
    
  
    



    
    
