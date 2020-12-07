import json
import socket
import time
import csv
import sys
import threading

'''
This file is used to simulate task execution by workers.
It listens to tasks requests from master and sends back updates.
It can be run multiple times to simulate multiple workers
It also logs all its task execution times for further analysis
'''

port = int(sys.argv[1])         # Port number it listens for tasks from master
worker_id = int(sys.argv[2])    # id number of the worker
algo = sys.argv[3]              # The scheduling algorithm to easily differentiate between log files

# Creating a log file to log the execution times for the tasks it executes
logfile=str(worker_id)+"_log_file_"+ algo+".csv"
with open(logfile, "w+") as f:
    w = csv.writer(f)
    w.writerow(["task_Id","time"])

file_lock = threading.Lock()

def listen_from_master():
    # This function listens to task requests from the master 
    s=socket.socket()
    worker_host='localhost'
    worker_port=port
    s.bind((worker_host,worker_port))
    s.listen(5)
    while(True):
        connection,address=s.accept()
        msg=connection.recv(2048).decode()

        if(msg != ""):
            # Decode the task given to it by master
            requests = json.loads(msg)
            job_id = requests["jobId"]
            task_id = requests["task_id"]
            interval = requests["interval"]
            print("Received : ", job_id, task_id, interval)

            # Notes the time at which the task was received
            start_time= time.time()
            # Start a thread to perform the task
            t2 = threading.Thread(target=send_to_master, args=(job_id, task_id, interval,start_time)) 
            t2.start()
        connection.close()



def send_to_master(job_id, task_id, interval,start_time):
    # function to send the response to master
    global worker_id
    time.sleep(interval)
    print("After execution of",job_id, task_id, interval)
    s=socket.socket()
    worker_host='localhost'
    master_port=5001
    end_time=time.time()
    # Calculate time taken to complete the task
    total_time=end_time- start_time 
    finish = {"workerId":worker_id,"jobId":job_id,"taskId":task_id}
    # Log the time taken to execute the task
    file_lock.acquire()
    with open(logfile,"a") as f:
        w = csv.writer(f)
        w.writerow([task_id,total_time])
    file_lock.release()
    # Update the master that the job has been completed
    s.connect((worker_host,master_port))  
    s.send(json.dumps(finish).encode())
    s.close()


# Start listening to requests from master
from_master=threading.Thread(target=listen_from_master())
from_master.start()

