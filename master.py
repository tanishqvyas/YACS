import json
import socket
import time
import sys
import random
import numpy
import threading
import csv

jobs_lock = threading.Lock()
worker_lock = threading.Lock()
task_lock = threading.Lock()
log_lock = threading.Lock()


#---------- Custom Imports -------------#

def listen_job_request():
    global JOBS
    master_host='localhost'
    master_port=5000
    master=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    master.bind((master_host,master_port))
    master.listen(1)
    while True:
        job,address=master.accept()
        request_json=job.recv(2048).decode()
        # NEED TO STORE TIME OF ARRIVAL FOR ANALYSIS LATER
        requests=json.loads(request_json)
        jobId = requests['job_id']
        
        print(requests)

        # Extractinnng the values for a Job
        total_map_tasks=len(requests['map_tasks'])
        total_reduce_tasks=len(requests['reduce_tasks'])
        
        total_completed_map_tasks=0
        total_completed_reduce_tasks=0

        list_map_tasks=requests['map_tasks']
        list_reduce_tasks=requests['reduce_tasks']

        job_arrival_time=time.time()
        
        # Creating a dict to append
        job_to_append = {
            "total_map_tasks":total_map_tasks,
            "total_reduce_tasks":total_reduce_tasks,
            "total_completed_map_tasks":total_completed_map_tasks,
            "total_completed_reduce_tasks":total_completed_reduce_tasks,
            "list_map_tasks":list_map_tasks,
            "list_reduce_tasks":list_reduce_tasks,
            "job_arrival_time":job_arrival_time,
            "jobId": jobId
        }
        
        # Appending the job
        jobs_lock.acquire()
        JOBS.append(job_to_append)
        jobs_lock.release()

        job.close()



def listen_worker_update():
    
    #worker_id, job_is, task_id
    global WORKER_AVAILABILITY
    # Request received from worker
    master=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    master.bind(('localhost',5001))
    master.listen(100)
    while True:
        worker, address = master.accept()
        response=worker.recv(2048).decode()
        #worker id, job id, task id
        response=json.loads(response)
        to_remove=-1

        print("Received : ", response)
        # print(JOBS)

        jobs_lock.acquire()
        length=len(JOBS)
        jobs_lock.release()

        for i in range(length):
            print("yo",JOBS[i],response, sep="   <--->   ")
            jobs_lock.acquire()
            job_i=JOBS[i]
            jobs_lock.release()
            if job_i["jobId"]==response["jobId"]:
                
                print("yo4")
                
                if 'M' in response["taskId"]:
                # Map job
                    jobs_lock.acquire()
                    JOBS[i]["total_completed_map_tasks"]+=1
                    jobs_lock.release()
                    print("yo2")
                    break
                else:
                    jobs_lock.acquire()
                    JOBS[i]["total_completed_reduce_tasks"]+=1
                    jobs_lock.release()
                    print("yo3")

                    jobs_lock.acquire()
                    if JOBS[i]["total_completed_reduce_tasks"] == JOBS[i]["total_reduce_tasks"]:
                        jobs_lock.release()
                        to_remove=i
                        break
                    else:
                        jobs_lock.release()
                    
        if to_remove !=-1:

            print("JOB : ",response["jobId"], " Completed.")
            jobs_lock.acquire()
            total_time=time.time() - JOBS[to_remove]["job_arrival_time"]
            jobs_lock.release()
        #with open(logfile,"a") as f:
            w = csv.writer(f)
            w.writerow([response["jobId"],total_time])
            jobs_lock.acquire()
            JOBS.pop(to_remove)
            jobs_lock.release()

        worker_lock.acquire()
        WORKER_AVAILABILITY[response["workerId"]]["slots"]+=1
        worker_lock.release()

    worker.close()


# Function to Schedule TASKS
def send_job_to_worker():
    
    global SCHEDUELING_ALGO
    global WORKER_AVAILABILITY
    global JOBS

    # Extracting workers list in case of round robin
    cur_workers = list(WORKER_AVAILABILITY.keys())
    cur_workers.sort()
    cur_worker_idx = 0
    num_workers = 3

    while True:

        # Check if task available
        jobs_lock.acquire()
        length=len(JOBS)
        jobs_lock.release()
        while length==0:
            print("No Jobs left to schedule")
            jobs_lock.acquire()
            length=len(JOBS)
            jobs_lock.release()
            time.sleep(1)
            
        
        jobs_lock.acquire()
        if(len(JOBS) > 0):
            pass
        else:
            jobs_lock.release()
            continue   
        
        # Extracting task randomly 
        holder = JOBS[0]
        # JOBS.pop(0)
        # JOBS.append(holder)

        # -----------------------Checking What should be done for the task----------------

        # check if any map task left to schedule
        if(len(JOBS) > 0):
            pass
        else:
            jobs_lock.release()
            continue

        if(len(JOBS[-1]["list_map_tasks"]) > 0):
            
            task_to_send = {
                "jobId": JOBS[-1]["jobId"],
                "task_id": JOBS[-1]["list_map_tasks"][0]["task_id"],
                "interval": JOBS[-1]["list_map_tasks"][0]["duration"]
            }

            # Deletet the task scheduled
            JOBS[-1]["list_map_tasks"].pop(0)
           
        else:
            if(JOBS[-1]["total_map_tasks"] == JOBS[-1]["total_completed_map_tasks"]):

                if(len(JOBS[-1]["list_reduce_tasks"]) > 0):
                    # Schedule Reduce Tasks
                    task_to_send = {
                    "jobId": JOBS[-1]["jobId"],
                    "task_id": JOBS[-1]["list_reduce_tasks"][0]["task_id"],
                    "interval": JOBS[-1]["list_reduce_tasks"][0]["duration"]
                    }
                    
                    # Deletet the task scheduled
                    JOBS[-1]["list_reduce_tasks"].pop(0)
                
                # No reduce task left to schedule
                else:
                    if(JOBS[-1]["total_completed_reduce_tasks"] == JOBS[-1]["total_reduce_tasks"]):
                        task_to_send = {"msg":"ISSSUEEEEEEEEEEEEee"}
                        jobs_lock.release()
                        continue
                    else:
                        task_to_send = {"msg":"ISSSUEEEEEEEEEEEEee"}
                        jobs_lock.release()
                        continue

            # Continue if there are map tasks runnning but none left to schedule
            else:
                jobs_lock.release()
                continue
        
        jobs_lock.release()            


        # Random Schedueling
        if(SCHEDUELING_ALGO == "Random"):
            slot_found = False

            worker_lock.acquire()
            workers_list = list(WORKER_AVAILABILITY.keys())
            worker_lock.release()

            while(not slot_found):
                max_slot_worker = 0
                
                if(len(workers_list) > 0):
                    wid = random.choice(workers_list)
                else:
                    break

                worker_lock.acquire()
                if(WORKER_AVAILABILITY[wid]["slots"] > 0):                    
                    slot_found=True
                    max_slot_worker=wid
                worker_lock.release()

                # If Slot if Found Then Send the request
                if(slot_found):
                    
                    worker_lock.acquire()
                    #jobs_lock.acquire()
                    # Decrease Slot availability by 1
                    WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1

                    # Send the Request
                    s=socket.socket()
                    s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
                    s.send(json.dumps(task_to_send).encode())
                    print("Sending Task  :   ", task_to_send)
                    #jobs_lock.release()
                    worker_lock.release()
                    break

                else:
                    workers_list.remove(wid)


        # Round Robin Schedueling
        elif(SCHEDUELING_ALGO == "RR"):
            
            slot_found = False
            while(not slot_found):

                max_slot_worker = 0
                
                while not slot_found:

                    if(WORKER_AVAILABILITY[cur_workers[cur_worker_idx]]["slots"] > 0):
                        slot_found=True
                        max_slot_worker=cur_workers[cur_worker_idx]
                        cur_worker_idx = (cur_worker_idx+1)%num_workers
                        break
                    else:
                        cur_worker_idx = (cur_worker_idx+1)%num_workers

                
                # If Slot if Found Then Send the request
                if(slot_found):
                    
                    worker_lock.acquire()
                    jobs_lock.acquire()
                    # Decrease Slot availability by 1
                    WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1

                    # Send the Request
                    s=socket.socket()
                    s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
                    s.send(json.dumps(task_to_send).encode())
                    print("Sending Task  :   ", task_to_send)
                    jobs_lock.release()
                    worker_lock.release()
                    break
                    

                else:
                    print("No Slots Found. Sleeping for One Second")
                    time.sleep(1)
            


        # Least Loaded Schedueling
        elif SCHEDUELING_ALGO == "LL":
            slot_found = False
            max_slots = 0
            max_slot_worker = 0
            while(not slot_found):

                for wid, _ in WORKER_AVAILABILITY.items():

                    if(WORKER_AVAILABILITY[wid]["slots"] > max_slots): 
                        
                        max_slots = WORKER_AVAILABILITY[wid]["slots"]
                        max_slot_worker = wid
                        slot_found = True
                        
                
                # If Slot if Found Then Send the request
                if(slot_found):
                    
                    worker_lock.acquire()
                    jobs_lock.acquire()
                    # Decrease Slot availability by 1
                    WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1

                    # Send the Request
                    s=socket.socket()
                    s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
                    s.send(json.dumps(task_to_send).encode())
                    print("Sending Task  :   ", task_to_send)
                    jobs_lock.release()
                    worker_lock.release()
                    break
                    

                else:
                    print("No Slots Found. Sleeping for One Second")
                    time.sleep(1)




# Reading the command line arguments
PATH_TO_CONFIG = sys.argv[1]
SCHEDUELING_ALGO = sys.argv[2]
JOBS = []


# Creating log files for Master
master_begins = time.time()

if sys.argv[2] == "Random":
    logfile = "Masterlogs_Random.csv"
elif sys.argv[2] == "RR":
    logfile = "Masterlogs_RR.csv"
elif sys.argv[2] == "LL":
    logfile = "Masterlogs_LL.csv"

f = open(logfile,'w+')
w = csv.writer(f)
w.writerow(["Job_Id","Time"])



# Reading the config.json
config_file = open(PATH_TO_CONFIG,)
configuration = json.load(config_file)

# Initialized Worker Slots Availability
WORKER_AVAILABILITY = dict()
for worker in configuration["workers"]:
    WORKER_AVAILABILITY[worker["worker_id"]] = {}
    WORKER_AVAILABILITY[worker["worker_id"]]["slots"] = worker["slots"]
    WORKER_AVAILABILITY[worker["worker_id"]]["port"] = worker["port"]

print(WORKER_AVAILABILITY)

# Start the thread to listen to jobs
job_listening_thread = threading.Thread(target = listen_job_request)
job_listening_thread.start()

job_scheduling_thread = threading.Thread(target = send_job_to_worker)
job_scheduling_thread.start()

worker_updates_thread = threading.Thread(target = listen_worker_update)
worker_updates_thread.start()
