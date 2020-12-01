import json
import socket
import time
import sys
import random
import numpy

class worker_job:
    print("done done")
    def __init__(self):#,id,interval):
        self.free_slots=6
        #self.slots=[]
        #self.interval=interval
        #self.id=id
        self.exec_pool=[]

    def new_job():
        self.exec_pool.append(id)
        print('done')
        
    def run_task(id,interval):    
        for i in range(self.interval):
            i-=1
            time.sleep(1)
        self.exec_pool.pop(id)
        print('done')
        

# Create 3 threads for 3 workers
a=worker_job()
a.new_job(9)
a.run_task(9,3)