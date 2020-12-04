import json
import socket
import time
import sys
import random
import numpy
import threading
import csv
'''
This file is used to run the master for YACS.
It listens to incoming job requests and schedules the tasks to different slots on workers
It uses RR, LL and Random scheduling algorithms to do the same
It also logs task and job execution details for further analysis
'''

# Locks to protect shared variables
jobs_lock = threading.Lock()
worker_lock = threading.Lock()
task_lock = threading.Lock()
log_lock = threading.Lock()

# Master listens to job requests from port 5000
def listen_job_request():
    global JOBS
    # This host name can be changed if the the worker and master are on different machines
    master_host='localhost'
    master_port=5000
    master=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    master.bind((master_host,master_port))
    master.listen(1)
    while True:
        job,address=master.accept()
        request_json=job.recv(2048).decode()
        # Request is loaded
        requests=json.loads(request_json)
        jobId = requests['job_id']
        
        print(requests)

        # Extracting the values of attributes for a Job
        total_map_tasks=len(requests['map_tasks'])
        total_reduce_tasks=len(requests['reduce_tasks'])

        # Variables to store the number of completed map and reduce tasks
        total_completed_map_tasks=0
        total_completed_reduce_tasks=0

        list_map_tasks=requests['map_tasks']
        list_reduce_tasks=requests['reduce_tasks']

        job_arrival_time=time.time()
        
        # Creating a job with various features
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
        
        # Appending the dictionary to list of jobs
        jobs_lock.acquire()
        JOBS.append(job_to_append)
        jobs_lock.release()

        job.close()


# Master listens to updates from workers
def listen_worker_update():

    global WORKER_AVAILABILITY
    # Request received from worker
    worker_lock.acquire()
    cur_workers = list(WORKER_AVAILABILITY.keys())
    worker_lock.release()
    cur_workers.sort()
    cur_worker_idx = 0
    num_workers = 3 # This value can be increased if more workers are present

    master=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    master.bind(('localhost',5001))
    master.listen(100)
    while True:
        worker, address = master.accept()
        # Task completion message received from worker
        response=worker.recv(2048).decode()
        # Response contains worker id, job id, task id
        response=json.loads(response)
        to_remove=-1

        print("Received : ", response)

        jobs_lock.acquire()
        length=len(JOBS)
        jobs_lock.release()

        for i in range(length):

            jobs_lock.acquire()
            if JOBS[i]["jobId"]==response["jobId"]:
                jobs_lock.release()
                # Checking if job completed is a map job
                if 'M' in response["taskId"]:
                    # Decrease count of number of map jobs
                    jobs_lock.acquire()
                    JOBS[i]["total_completed_map_tasks"]+=1
                    jobs_lock.release()
                    break
                else:
                    # Not map job => Reduce job
                    # Decrease count of number of reduce jobs
                    jobs_lock.acquire()
                    JOBS[i]["total_completed_reduce_tasks"]+=1
                    jobs_lock.release()

                    # If all reduce jobs are completed
                    jobs_lock.acquire()
                    if JOBS[i]["total_completed_reduce_tasks"] == JOBS[i]["total_reduce_tasks"]:
                        jobs_lock.release()
                        # Job has to be removed from jobs list
                        to_remove=i
                        break
                    else:
                        jobs_lock.release()
            else:
                jobs_lock.release()
                    
        if to_remove !=-1:
            # Job has been completed and must be removed from the list of jobs
            print("JOB : ",response["jobId"], " Completed.")
            jobs_lock.acquire()
            # Calculating job execution time
            total_time=time.time() - JOBS[to_remove]["job_arrival_time"]
            jobs_lock.release()

            with open(logfile,"a") as f:
                w = csv.writer(f)
                w.writerow([response["jobId"],total_time])
            jobs_lock.acquire()
            # Remove job from jobs list
            JOBS.pop(to_remove)
            jobs_lock.release()

        # Modifying number of free slots in the worker
        worker_lock.acquire()
        WORKER_AVAILABILITY[response["workerId"]]["slots"]+=1
        worker_lock.release()
        time_recv=time.time()
        for i in range(num_workers):
            with open("tasks_"+logfile,'a') as f:
                w = csv.writer(f)
                worker_lock.acquire()
                w.writerow([cur_workers[i],WORKER_AVAILABILITY[cur_workers[i]]["maxcapacity"]-WORKER_AVAILABILITY[cur_workers[i]]["slots"],time_recv])
                worker_lock.release()
                    

    worker.close()


# Function to Schedule TASKS
def send_job_to_worker():
    
    global SCHEDUELING_ALGO
    global WORKER_AVAILABILITY
    global JOBS

    # Extracting workers list in case of round robin
    worker_lock.acquire()
    cur_workers = list(WORKER_AVAILABILITY.keys())
    worker_lock.release()
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

        # Jobs are scheduled in FCFS manner
        holder = JOBS[0]

        # -----------------------Checking What should be done for the task----------------

        # check if any map task left to schedule
        if(len(JOBS) > 0):
            pass
        else:
            jobs_lock.release()
            continue

        # If map tasks are yet to be sent to workers
        if(len(JOBS[0]["list_map_tasks"]) > 0):
            
            task_to_send = {
                "jobId": JOBS[0]["jobId"],
                "task_id": JOBS[0]["list_map_tasks"][0]["task_id"],
                "interval": JOBS[0]["list_map_tasks"][0]["duration"]
            }

            # Delete the task that has been scheduled
            JOBS[0]["list_map_tasks"].pop(0)
           
        else:
            # If all map tasks have been completed i.e. no map tasks is being executed by a worker
            if(JOBS[0]["total_map_tasks"] == JOBS[0]["total_completed_map_tasks"]):

                if(len(JOBS[0]["list_reduce_tasks"]) > 0):
                    # Schedule Reduce Tasks
                    task_to_send = {
                    "jobId": JOBS[0]["jobId"],
                    "task_id": JOBS[0]["list_reduce_tasks"][0]["task_id"],
                    "interval": JOBS[0]["list_reduce_tasks"][0]["duration"]
                    }
                    
                    # Delete the task scheduled
                    JOBS[0]["list_reduce_tasks"].pop(0)
                
                # No reduce task left to schedule
                else:
                    if(JOBS[0]["total_completed_reduce_tasks"] == JOBS[0]["total_reduce_tasks"]):
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


        # Random Scheduling
        if(SCHEDUELING_ALGO == "Random"):
            slot_found = False

            workers_list = list(WORKER_AVAILABILITY.keys())

            # Randomly choosing a worker
            while(not slot_found):
                max_slot_worker = 0
                
                if(len(workers_list) > 0):
                    wid = random.choice(workers_list)
                else:
                    break

                worker_lock.acquire()
                # Check if chosen worker has free slot
                if(WORKER_AVAILABILITY[wid]["slots"] > 0):                     
                    slot_found=True
                    max_slot_worker=wid
                worker_lock.release()
                
                # If Slot if Found Then Send the request
                if(slot_found):
                    
                    worker_lock.acquire()
                    # Decrease Slot availability by 1
                    WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1

                    # Send the Request
                    s=socket.socket()
                    s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
                    s.send(json.dumps(task_to_send).encode())
                    print("Sending Task  :   ", task_to_send)

                    time_sent=time.time()
                    for i in range(num_workers):
                        with open("tasks_"+logfile,'a') as f:
                            w = csv.writer(f)
                            w.writerow([cur_workers[i],WORKER_AVAILABILITY[cur_workers[i]]["maxcapacity"]-WORKER_AVAILABILITY[cur_workers[i]]["slots"],time_sent])
                    
                    worker_lock.release()
                    break

                else:
                    workers_list.remove(wid)


        # Round Robin Scheduling
        elif(SCHEDUELING_ALGO == "RR"):
            
            slot_found = False
            while(not slot_found):

                max_slot_worker = 0
                
                while not slot_found:
                    worker_lock.acquire()
                    if(WORKER_AVAILABILITY[cur_workers[cur_worker_idx]]["slots"] > 0):
                        worker_lock.release()
                        slot_found=True
                        max_slot_worker=cur_workers[cur_worker_idx]
                        cur_worker_idx = (cur_worker_idx+1)%num_workers
                        break
                    else:
                        worker_lock.release()
                        cur_worker_idx = (cur_worker_idx+1)%num_workers

                
                # If Slot if Found Then Send the request
                if(slot_found):                    
                    # Decrease Slot availability by 1
                    worker_lock.acquire()
                    WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1

                    # Send the Request
                    s=socket.socket()
                    s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
                    s.send(json.dumps(task_to_send).encode())
                    print("Sending Task  :   ", task_to_send)

                    time_sent=time.time()
                    for i in range(num_workers):
                        with open("tasks_"+logfile,'a') as f:
                            w = csv.writer(f)
                            w.writerow([cur_workers[i],WORKER_AVAILABILITY[cur_workers[i]]["maxcapacity"]-WORKER_AVAILABILITY[cur_workers[i]]["slots"],time_sent])
                    
                    worker_lock.release()
                    break
                    

                else:
                    print("No Slots Found. Sleeping for One Second")
                    time.sleep(1)
            


        # Least Loaded Scheduling
        elif SCHEDUELING_ALGO == "LL":
            slot_found = False
            max_slots = 0
            max_slot_worker = 0
            while(not slot_found):

                # Find which worker is least loaded - i.e. Has maximum number of free slots
                for wid, _ in WORKER_AVAILABILITY.items():

                    if(WORKER_AVAILABILITY[wid]["slots"] > max_slots): 
                        
                        max_slots = WORKER_AVAILABILITY[wid]["slots"]
                        max_slot_worker = wid
                        slot_found = True
                        
                
                # If Slot if Found Then Send the request
                if(slot_found):
                    
                    worker_lock.acquire()
                    # Decrease Slot availability by 1
                    WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1

                    # Send the Request
                    s=socket.socket()
                    s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
                    s.send(json.dumps(task_to_send).encode())
                    print("Sending Task  :   ", task_to_send)
                    time_sent=time.time()
                    for i in range(num_workers):
                        with open("tasks_"+logfile,'a') as f:
                            w = csv.writer(f)
                            w.writerow([cur_workers[i],WORKER_AVAILABILITY[cur_workers[i]]["maxcapacity"]-WORKER_AVAILABILITY[cur_workers[i]]["slots"],time_sent])
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

with open(logfile,'w+') as f:
    w = csv.writer(f)
    w.writerow(["Job_Id","Time"])

with open("tasks_"+logfile,'w+') as f:
    w = csv.writer(f)
    w.writerow(["Worker_id","No_Tasks","Time"])



# Reading the config.json
config_file = open(PATH_TO_CONFIG,)
configuration = json.load(config_file)

# Initialized Worker Slots Availability
WORKER_AVAILABILITY = dict()
for worker in configuration["workers"]:
    WORKER_AVAILABILITY[worker["worker_id"]] = {}
    WORKER_AVAILABILITY[worker["worker_id"]]["slots"] = worker["slots"]
    WORKER_AVAILABILITY[worker["worker_id"]]["port"] = worker["port"]
    WORKER_AVAILABILITY[worker["worker_id"]]["maxcapacity"] = worker["slots"]

print(WORKER_AVAILABILITY)

# Start the thread to listen to jobs
job_listening_thread = threading.Thread(target = listen_job_request)
job_listening_thread.start()

# Schedules tasks and sends them to worker for completion
job_scheduling_thread = threading.Thread(target = send_job_to_worker)
job_scheduling_thread.start()

# Listens to update from workers about completed tasks
worker_updates_thread = threading.Thread(target = listen_worker_update)
worker_updates_thread.start()