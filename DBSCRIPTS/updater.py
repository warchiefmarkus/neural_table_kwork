# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 20:00:12 2020

@author: Kravchik
"""

import schedule
import time
from subprocess import call

def update():
    print("START UPDATE "+time.strftime("%H:%M:%S", time.gmtime()))
    call(["python", "super_table.py"])
    print("UPDATE END "+time.strftime("%H:%M:%S", time.gmtime()))
    
    
schedule.every(40000).minutes.do(update)    
    
try:
    while True:
        schedule.run_pending()
        time.sleep(300) 
except KeyboardInterrupt:
    print("INTERUPT")
    pass