import json
import socket
import time
import sys
import random
import numpy
import threading

lock = threading.Lock()

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
        job_completion_time=-1
        
        # Creating a dict to append
        job_to_append = {
            "total_map_tasks":total_map_tasks,
            "total_reduce_tasks":total_reduce_tasks,
            "total_completed_map_tasks":total_completed_map_tasks,
            "total_completed_reduce_tasks":total_completed_reduce_tasks,
            "list_map_tasks":list_map_tasks,
            "list_reduce_tasks":list_reduce_tasks,
            "job_arrival_time":job_arrival_time,
            "job_completion_time":job_completion_time,
            "jobId": jobId
        }

        # Appendingh the job
        JOBS.append(job_to_append)

        job.close()



def listen_worker_update():

    #worker_id, job_is, task_id
    global WORKER_AVAILABILITY
    # Request received from worker
    master=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    master.bind(('localhost',5001))
    master.listen(100)
    while True:
        worker,address=master.accept()
        response=worker.recv(2048).decode()
        #worker id, job id, task id
        response=json.loads(response)
        to_remove=-1

        print("Received : ", response)

        for i in range(len(JOBS)):
            if JOBS[i]["jobId"]==response["jobId"]:
                if 'M' in response["taskId"]:
                # Map job
                    lock.acquire()
                    JOBS[i]["total_completed_map_tasks"]+=1
                    lock.release()
                    break
                else:
                    JOBS[i]["total_completed_reduce_tasks"]+=1
                    if JOBS[i]["total_completed_reduce_tasks"] == JOBS[i]["total_reduce_tasks"]:
                        to_remove=i
                        break
        if to_remove !=-1:
            #total_time=time.time() - JOBS[to_remove]["job_arrival_time"]
            lock.acquire()
            JOBS.pop(to_remove)
            lock.release()

        lock.acquire()
        WORKER_AVAILABILITY[response["workerId"]]["slots"]+=1
        lock.release()

    worker.close()


# Function to Schedule TASKS
def send_job_to_worker():
    
    global SCHEDUELING_ALGO
    global WORKER_AVAILABILITY
    global JOBS

    while True:

        # Check if task available
        while len(JOBS)==0:
            print("No tasks left to schedule")
            time.sleep(1)
        
        # Extracting task randomly 
        lock.acquire()       
        cur_task_to_send = JOBS[0]
        JOBS.pop(0)
        JOBS.append(cur_task_to_send)
        lock.release()

        
        # -----------------------Checking What should be done for the task----------------

        # check if any map task left to schedule
        if(len(cur_task_to_send["list_map_tasks"]) > 0):
            
            task_to_send = {
                "jobId": cur_task_to_send["jobId"],
                "task_id": cur_task_to_send["list_map_tasks"][0]["task_id"],
                "interval": cur_task_to_send["list_map_tasks"][0]["duration"]
            }

            # Deletet the task scheduled
            cur_task_to_send["list_map_tasks"].pop(0)

        else:
            if(cur_task_to_send["total_map_tasks"] == cur_task_to_send["total_completed_map_tasks"]):

                if(len(cur_task_to_send["list_reduce_tasks"]) > 0):
                    # Schedule Reduce Tasks
                    task_to_send = {
                    "jobId": cur_task_to_send["jobID"],
                    "task_id": cur_task_to_send["list_reduce_tasks"][0]["task_id"],
                    "interval": cur_task_to_send["list_reduce_tasks"][0]["duration"]
                    }
                    
                    # Deletet the task scheduled
                    cur_task_to_send["list_reduce_tasks"].pop(0)
                
                # No reduce task left to schedule
                else:
                    if(cur_task_to_send["total_completed_reduce_tasks"] == cur_task_to_send["total_reduce_tasks"]):
                        continue
                    else:
                        continue
                    



        # Random Schedueling
        if(SCHEDUELING_ALGO == "Random"):
            slot_found = False

            workers_list = list(WORKER_AVAILABILITY.keys())

            while(not slot_found):
                max_slot_worker = 0
                
                if(len(workers_list) > 0):
                    wid = random.choice(workers_list)
                else:
                    break

                if(WORKER_AVAILABILITY[wid]["slots"] > 0): 
                    slot_found=True
                    max_slot_worker=wid
                
                # If Slot if Found Then Send the request
                if(slot_found):
                    
                    lock.acquire()
                    # Decrease Slot availability by 1
                    WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1

                    # Send the Request
                    s=socket.socket()
                    s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
                    s.send(json.dumps(task_to_send).encode())
                    print("Skadoosh : ", task_to_send)
                    JOBS.pop(0)
                    lock.release()
                    break

                else:
                    workers_list.remove(wid)


        # Round Robin Schedueling
        elif(SCHEDUELING_ALGO == "RR"):
            
            slot_found = False
            while(not slot_found):

                max_slot_worker = 0
                cur_workers = list(WORKER_AVAILABILITY.keys())
                cur_workers.sort()

                for wid in cur_workers:

                    if(WORKER_AVAILABILITY[wid]["slots"] > 0): 
                        slot_found=True
                        max_slot_worker=wid
                        break
                
                # If Slot if Found Then Send the request
                if(slot_found):
                    
                    lock.acquire()
                    # Decrease Slot availability by 1
                    WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1

                    # Send the Request
                    s=socket.socket()
                    s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
                    s.send(json.dumps(task_to_send).encode())
                    JOBS.pop(0)
                    lock.release()
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
                    
                    lock.acquire()
                    # Decrease Slot availability by 1
                    WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1

                    # Send the Request
                    s=socket.socket()
                    s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
                    s.send(json.dumps(task_to_send).encode())
                    JOBS.pop(0)
                    lock.release()
                    break
                    

                else:
                    print("No Slots Found. Sleeping for One Second")
                    time.sleep(1)




# Reading the command line arguments
PATH_TO_CONFIG = sys.argv[1]
SCHEDUELING_ALGO = sys.argv[2]
JOBS = []


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
