import json
import socket
import time
import sys
import random
import numpy
import threading

sem=threading.Semaphore(1)
port = int(sys.argv[1])
worker_id = int(sys.argv[2])

execution_pool=dict()

def listen_from_master():
    s=socket.socket()
    worker_host='localhost'
    worker_port=port
    s.bind((worker_host,worker_port))
    s.listen(1)
    connection,address=s.accept()
    #while(1):
    #s.sendall(worker_id.encode())
    while(True):
        msg=connection.recv(2048).decode()
        if(msg):
            requests = json.loads(msg)
            job_id = requests["jobId"]
            task_id = requests["taskId"]
            interval = requests["interval"]
            sem.acquire()
            execution_pool[job_id,task_id]=int(interval)
            sem.release()
    s.close()


def send_to_master():
    s=socket.socket()
    worker_host='localhost'
    master_port=5001
    s.connect((master_host,master_port))
    while(1):
        for i in execution_pool:
            execution_pool[i]-=1
            if(execution_pool[i]<=0):        
                del execution_pool[i]
                finish = {"workerId":worker_id,"jobId":job_id,"taskId":task_id}
                s.sendall(json.dumps(finish).encode())
                #sent id to master using thread2
        time.sleep(1)
    s.close()


from_master=threading.Thread(target=listen_from_master())
to_master=threading.Thread(target=send_to_master())

from_master.start()
to_master.start()

from_master.join()
to_master.join()

'''
#task_id = 0
# Listen to master
# get id, interval from master
# Lock the exec pool when youre adding to list
execution_pool[task_id]=interval
task_id = task_id+1

while(1):
    for i in execution_pool:
        execution_pool[i]-=1
        if(execution_pool[i]==0):        
            del execution_pool[i]
            #sent id to master using thread2
    time.sleep(1)

'''

'''
#As it listens to request - create thread (bcuz master keeps track of number of slots free - so safe to create thread)
def call_worker(id,interval):
    t1=threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
class worker_job:
    #print("done done")
    def __init__(self):#,id,interval):
        self.free_slots=6
        #self.slots=[]
        #self.interval=interval
        #self.id=id
        self.exec_pool=[]

    def new_job(self,id):
        self.exec_pool.append(id)
        print('done')
        
    def run_task(self,id,interval):    
        for i in range(interval):
            i-=1
            time.sleep(1)
        self.exec_pool.remove(id)
        print('done')
        

# Create 3 threads for 3 workers
a=worker_job()
a.new_job(9)
a.run_task(9,3)
'''
