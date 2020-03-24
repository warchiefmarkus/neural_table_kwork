# -*- coding: utf-8 -*-
import requests, time
from multiprocessing.dummy import Pool as ThreadPool
from itertools import groupby

start_time = time.clock()

def foo(pair):
    return requests.get(pair[0]+pair[1])

urls = [
	['http://www.python','.org'],
	['http://www.python.org/','about/'],
	['http://www.onlamp.com/pub/a/python/2003/04/17/','metaclasses.html'],
	['http://www.python.org','/doc/'],
	['http://www.python.org/','download/']]

pool = ThreadPool(4)
results = pool.map(foo, urls)

pool.close()
pool.join()

print (time.clock() - start_time, "seconds")





start_time = time.clock()

for i in urls:
    requests.get(i[0]+i[1])

print (time.clock() - start_time, "seconds")
#%%

def tab_sort(list_d):
        list_l = list_d.copy()
        l = []
        pp = []
        id_i = 0
        for index, el_1 in enumerate(list_l):
            if all(el_0 != el_1 for el_0 in l):
                l.append(el_1)
                k = 0
                kj = id_i + 1
                p = [id_i]
                for el_2 in list_l[id_i + 1:len(list_l)]:
                    if el_2 == el_1:
                        p.append(kj)
                        l.append(el_2)
                    kj += 1
                    k += 1
                pp.append(p)
            id_i += 1
        keys = [list(groupby(l))[i][0] for i in range(len(list(groupby(l))))]
        dict = {str(keys[i]): pp[i] for i in range(len(keys))}
        return [dict, keys, pp, l]

def sorted_pair(pair): 
    print("tab")
    list2 = pair[0]
    list1 = pair[1]
    ll = tab_sort(list1)
    numer = [ll[2][i][j] for i in range(len(ll[2])) for j in range(len(ll[2][i]))]
    list3 = [list2[numer[i]] for i in range(len(list2))]
    return list3 #DEBUG

def sorted_personal(table):    
    
    map_list = []
    for k in table.keys():
        map_list.append([table.get(k), table.get('TabNum')]) #DEBUG
    
    pool = ThreadPool(4)
    results = pool.map(sorted_pair, map_list)
    
    pool.close()
    pool.join()

    return results

start_time = time.clock()

res = sorted_personal(d)

print (time.clock() - start_time, "seconds")



