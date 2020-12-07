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
file_lock = threading.Lock()

# Master listens to job requests from port 5000
def listen_job_request():
    global JOBS

    # This host name can be changed if the the worker and master are on different machines
    master_host='localhost'
    master_port=5000
    master=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    master.bind((master_host,master_port))
    master.listen(10)

    # Loop to continue listening to incoming jobs requests
    while True:

        # Request is loaded
        job, address=master.accept()
        request_json=job.recv(2048).decode()
        requests=json.loads(request_json)
        
        print("\nJob Request : ", requests,"\n")
        
        # Extracting the values of attributes for a Job
        jobId = requests['job_id']
        total_map_tasks=len(requests['map_tasks'])
        total_reduce_tasks=len(requests['reduce_tasks'])

        # Variables to store the number of completed map and reduce tasks
        total_completed_map_tasks=0
        total_completed_reduce_tasks=0

        list_map_tasks=requests['map_tasks']
        list_reduce_tasks=requests['reduce_tasks']

        job_arrival_time=time.time()
        
        # Creating a dictionary to append
        # Dictionary contains 
            # Total number of map and reduce tasks
            # Number of completed map and reduce tasts
            # List of map and reduced tasks
            # Job arrival time
            # Job id
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

    # Fetching the list of available workers
    cur_workers = list(WORKER_AVAILABILITY.keys())
    cur_workers.sort()
    cur_worker_idx = 0
    num_workers = 3 # This value can be increased if more workers are present

    master=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    master.bind(('localhost',5001))
    master.listen(100)

    # Loop to continue listening updates from all workers
    while True:
        worker, address = master.accept()
        # Task completion message received from worker
        response=worker.recv(2048).decode()
        # Response contains worker id, job id, task id
        response=json.loads(response)
        
        # Variable to handle job removal in case of completion
        to_remove=-1

        print("Received : ", response)


        jobs_lock.acquire()

        # Process the Completion Update from the worker
        for i in range(len(JOBS)):
            
            # for matching job:task
            if JOBS[i]["jobId"]==response["jobId"]:
                
                # Checking if job completed is a map job
                if 'M' in response["taskId"]:
                    # Decrease count of number of map jobs
                    JOBS[i]["total_completed_map_tasks"]+=1
                    break

                # Reduce Job
                else:
                    # Not map job => Reduce job
                    # Decrease count of number of reduce jobs
                    JOBS[i]["total_completed_reduce_tasks"]+=1

                    # If all reduce jobs are completed
                    if JOBS[i]["total_completed_reduce_tasks"] == JOBS[i]["total_reduce_tasks"]:
                        # Job has to be removed from jobs list
                        to_remove=i
                        break

        # Removal of the completed job from JOBS list
        if to_remove !=-1:
            print("\n#############################################\nJOB : ",response["jobId"], " Completed.\n#############################################\n")
            
            # Calculating job execution time
            total_time=time.time() - JOBS[to_remove]["job_arrival_time"]
            
            # logging master logs
            with open(logfile,"a") as f:
                w = csv.writer(f)
                w.writerow([response["jobId"],total_time])
            # Remove job from jobs list
            JOBS.pop(to_remove)

        jobs_lock.release()


        
        # Modifying number of free slots in the worker        
        worker_lock.acquire()
        WORKER_AVAILABILITY[response["workerId"]]["slots"]+=1
        worker_lock.release()

        file_lock.acquire()
        # logging task execution times for different workers       
        time_recv=time.time()
        for i in range(num_workers):
            with open("tasks_"+logfile,'a') as f:
                w = csv.writer(f)
                w.writerow([cur_workers[i],WORKER_AVAILABILITY[cur_workers[i]]["maxcapacity"]-WORKER_AVAILABILITY[cur_workers[i]]["slots"],time_recv])
        file_lock.release()
    worker.close()


# Function to Schedule TASKS using different algorithms
def send_job_to_worker():
    
    global SCHEDULING_ALGO
    global WORKER_AVAILABILITY
    global JOBS

    # Extracting workers list in case of round robin
    cur_workers = list(WORKER_AVAILABILITY.keys())
    cur_workers.sort()
    cur_worker_idx = 0
    num_workers = 3

    while True:

        # Check if JOBS available
        while len(JOBS)==0:
            print("No Jobs left to schedule")
            length=len(JOBS)
            time.sleep(1)


        jobs_lock.acquire()

        # Schedueling Jobs in Random Fashion
        if len(JOBS)>0:
            job_to_schedule = random.randint(0, len(JOBS)-1)
        else:
            jobs_lock.release()
            continue

        # If map tasks are yet to be sent to workers
        if(len(JOBS[job_to_schedule]["list_map_tasks"]) > 0):
            
            task_to_send = {
                "jobId": JOBS[job_to_schedule]["jobId"],
                "task_id": JOBS[job_to_schedule]["list_map_tasks"][0]["task_id"],
                "interval": JOBS[job_to_schedule]["list_map_tasks"][0]["duration"]
            }

            # Delete the task that has been scheduled
            
            JOBS[job_to_schedule]["list_map_tasks"].pop(0)

        else:
            # If all map tasks have been completed i.e. no map tasks is being executed by a worker
            if(JOBS[job_to_schedule]["total_map_tasks"] == JOBS[job_to_schedule]["total_completed_map_tasks"]):

                if(len(JOBS[job_to_schedule]["list_reduce_tasks"]) > 0):
                    # Schedule Reduce Tasks
                    task_to_send = {
                    "jobId": JOBS[job_to_schedule]["jobId"],
                    "task_id": JOBS[job_to_schedule]["list_reduce_tasks"][0]["task_id"],
                    "interval": JOBS[job_to_schedule]["list_reduce_tasks"][0]["duration"]
                    }
                    
                    # Delete the task scheduled
                    JOBS[job_to_schedule]["list_reduce_tasks"].pop(0)

                # No reduce task left to schedule
                else:
                    jobs_lock.release()
                    continue

            # Continue if there are map tasks runnning but none left to schedule
            else:
                jobs_lock.release()
                continue
        
        jobs_lock.release()

        # Random Scheduling
        if(SCHEDULING_ALGO == "Random"):
            slot_found = False

            workers_list = list(WORKER_AVAILABILITY.keys())

            # Randomly choosing a worker
            while(not slot_found):
                max_slot_worker = 0
                
                if(len(workers_list) > 0):
                    wid = random.choice(workers_list)
                else:
                    workers_list = list(WORKER_AVAILABILITY.keys())
                    # break

                # Check if chosen worker has free slot
                if(WORKER_AVAILABILITY[wid]["slots"] > 0):                     
                    slot_found=True
                    max_slot_worker=wid
                else:
                    workers_list.remove(wid)
                    continue
                
                # If Slot is Found Then Send the request
                if(slot_found):
                    
                    # Decrease Slot availability by 1
                    worker_lock.acquire()
                    WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1
                    worker_lock.release()

                    # Send the Request
                    s=socket.socket()
                    s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
                    s.send(json.dumps(task_to_send).encode())
                    print("Sending Task  :   ", task_to_send)

                    file_lock.acquire()
                    # Write to logs
                    time_sent=time.time()
                    for i in range(num_workers):
                        with open("tasks_"+logfile,'a') as f:
                            w = csv.writer(f)
                            w.writerow([cur_workers[i],WORKER_AVAILABILITY[cur_workers[i]]["maxcapacity"]-WORKER_AVAILABILITY[cur_workers[i]]["slots"],time_sent])
                    file_lock.release()


        # Round Robin Scheduling
        elif(SCHEDULING_ALGO == "RR"):
            
            slot_found = False
            max_slot_worker = 0
            
            # Search for a worker in Round Robin Fashion to schedule task
            while not slot_found:
                if(WORKER_AVAILABILITY[cur_workers[cur_worker_idx]]["slots"] > 0):
                    slot_found=True
                    max_slot_worker=cur_workers[cur_worker_idx]
                    cur_worker_idx = (cur_worker_idx+1)%num_workers
                else:
                    cur_worker_idx = (cur_worker_idx+1)%num_workers

            
            # If Slot if Found Then Send the request                
            # Decrease Slot availability by 1
            worker_lock.acquire()    
            WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1
            worker_lock.release()

            # Send the Request
            s=socket.socket()
            s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
            s.send(json.dumps(task_to_send).encode())
            print("Sending Task  :   ", task_to_send)

            file_lock.acquire()
            # Write to logs
            time_sent=time.time()
            for i in range(num_workers):
                with open("tasks_"+logfile,'a') as f:
                    w = csv.writer(f)
                    w.writerow([cur_workers[i],WORKER_AVAILABILITY[cur_workers[i]]["maxcapacity"]-WORKER_AVAILABILITY[cur_workers[i]]["slots"],time_sent])
            file_lock.release()

        # Least Loaded Scheduling
        elif SCHEDULING_ALGO == "LL":
            
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
                        

            # Decrease Slot availability by 1
            worker_lock.acquire()
            WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1
            worker_lock.release()

            # Send the Request
            s=socket.socket()
            s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
            s.send(json.dumps(task_to_send).encode())
            print("Sending Task  :   ", task_to_send)
            
            file_lock.acquire()
            # Write to logs
            time_sent=time.time()
            for i in range(num_workers):
                with open("tasks_"+logfile,'a') as f:
                    w = csv.writer(f)
                    w.writerow([cur_workers[i],WORKER_AVAILABILITY[cur_workers[i]]["maxcapacity"]-WORKER_AVAILABILITY[cur_workers[i]]["slots"],time_sent])
            file_lock.release()



# Reading the command line arguments
PATH_TO_CONFIG = sys.argv[1]            # Path to the configuration json file of the workers
SCHEDULING_ALGO = sys.argv[2]           # Name of the scheduling algorithm to be used - RR, LL or Random
JOBS = []                               # List of all jobs submitted to the master


# Time at which master begins execution
master_begins = time.time()

# Naming of Log file based on the scheduling algorithm
if sys.argv[2] == "Random":
    logfile = "Masterlogs_Random.csv"
elif sys.argv[2] == "RR":
    logfile = "Masterlogs_RR.csv"
elif sys.argv[2] == "LL":
    logfile = "Masterlogs_LL.csv"

# Creating log files for Master
with open(logfile,'w+') as f:
    w = csv.writer(f)
    w.writerow(["Job_Id","Time"])
# Creating log files for Tasks
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

print("Worker Availability : \n", WORKER_AVAILABILITY, "\n\n")


# Start the thread to listen to jobs
job_listening_thread = threading.Thread(target = listen_job_request)
job_listening_thread.start()

# Schedules tasks and sends them to worker for completion
job_scheduling_thread = threading.Thread(target = send_job_to_worker)
job_scheduling_thread.start()

# Listens to update from workers about completed tasks
worker_updates_thread = threading.Thread(target = listen_worker_update)
worker_updates_thread.start()