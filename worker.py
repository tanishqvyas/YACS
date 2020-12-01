import json
import socket
import time
import csv
import sys
import random
import numpy
import threading



sem=threading.Semaphore(1)
port = int(sys.argv[1])
worker_id = int(sys.argv[2])

logfile=str(worker_id)+"_log_file.csv"
f = open(logfile, "w+")
w = csv.writer(f)
w.writerow(["task_Id","time"])


def listen_from_master():
    s=socket.socket()
    worker_host='localhost'
    worker_port=port
    s.bind((worker_host,worker_port))
    s.listen(5)
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
            t2 = threading.Thread(target=send_to_master, args=(job_id, task_id, interval,time.time())) 
            t2.start()
        connection.close()


# function to send the response to master
def send_to_master(job_id, task_id, interval,start_time):
    global worker_id
    time.sleep(interval)
    print("After execution of",job_id, task_id, interval)
    s=socket.socket()
    worker_host='localhost'
    master_port=5001
    total_time=time.time() - start_time
    finish = {"workerId":worker_id,"jobId":job_id,"taskId":task_id}
    with open(logfile,"a") as f:
        w = csv.writer(f)
        w.writerow([task_id,total_time])
    s.connect((worker_host,master_port))  
    s.send(json.dumps(finish).encode())
    s.close()





# Start the worker
from_master=threading.Thread(target=listen_from_master())
from_master.start()

