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

execution_pool=list()

def listen_from_master():
    s=socket.socket()
    worker_host='localhost'
    worker_port=port
    s.bind((worker_host,worker_port))
    s.listen(100)
    #while(1):
    #s.sendall(worker_id.encode())
    while(True):
        connection,address=s.accept()
        msg=connection.recv(2048).decode()

        if(msg != ""):
            requests = json.loads(msg)
            job_id = requests["jobId"]
            task_id = requests["task_id"]
            interval = requests["interval"]
            print("Received : ", job_id, task_id, interval)
            # sem.acquire()
            execution_pool.append([job_id,task_id,float(interval)])
            # sem.release()
            print(execution_pool)
        
    s.close()

def send_to_master():

    time.sleep(2)
    global worker_id
    s=socket.socket()
    worker_host='localhost'
    master_port=5001
    while 1:
        try:
            s.connect((worker_host,master_port))
            break
        except:
            print("Trying to connect to master")

    print("Connection established with master to send back stufff")
    while(1):

        print("inside : " ,execution_pool)

        # sem.acquire()
        for task in range(len(execution_pool)):
            execution_pool[task][2]-=1
        
        to_delete = []
        for finished in range(len(execution_pool)):
            if execution_pool[finished][2]<=0:
                to_delete.append(execution_pool[finished])
                finish = {"workerId":worker_id,"jobId":execution_pool[finished][0],"taskId":execution_pool[finished][1]}
                s.send(json.dumps(finish).encode())
                
                print("Wapas bhej a hai maal : ", finish)

        for i in to_delete:
            print("Removed : ", i)
            execution_pool.remove(i)


        # sem.release()
        time.sleep(1)
    s.close()


from_master=threading.Thread(target=listen_from_master)
to_master=threading.Thread(target=send_to_master)

from_master.start()
to_master.start()

# from_master.join()
# to_master.join()
