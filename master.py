import json
import socket
import time
import sys
import random
import numpy
import threading

#---------- Custom Imports -------------#

def listen_job_request():
    global JOBS
    master_host='localhost'
    master_port=5000
    master=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    master.bind((master_host,master_port))
    master.listen(1)
    while True:
        job,address=s.accept()
        request_json=job.recv(1024)
        # NEED TO STORE TIME OF ARRIVAL FOR ANALYSIS LATER
        requests=json.loads(request_json)
        jobId = requests['job_id']
        
        for m in requests['map_tasks']:
            JOBS.append([m['task_id'], m['duration'],jobId])
        for r in requests['reduce_tasks']:
            JOBS.append([r['task_id'], r['duration'],jobId])

        job.close()



def listen_worker_update():

    #worker_id, job_is, task_id
    global WORKER_AVAILABILITY
    # Request received from worker
    master=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    master.bind('localhost',5001)
    master.listen(20)
    while True:
        worker,address=master.accept()
        response=worker.recv(1024)
        WORKER_AVAILABILITY[response["workerid"]]["slots"]+=1
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
        
        # Extracting task
        task_to_send={"jobId":JOBS[0][2],"taskId":JOBS[0][0],"interval":JOBS[0][1]}

        # Random Schedueling
        if(SCHEDUELING_ALGO == "Random"):
            slot_found = False

            workers_list = WORKER_AVAILABILITY.keys()

            while(not slot_found):
                max_slot_worker = 0
                
                wid = random.choice(workers_list)

                if(WORKER_AVAILABILITY[wid]["slots"] > 0): 
                    slot_found=True
                    max_slot_worker=wid
                
                # If Slot if Found Then Send the request
                if(slot_found):
                    
                    # Decrease Slot availability by 1
                    WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1

                    # Send the Request
                    s=socket.socket()
                    s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
                    s.send(json.dumps(task_to_send))
                    JOBS.pop(0)
                    break

                else:
                    workers_list.remove(wid)


        # Round Robin Schedueling
        elif(SCHEDUELING_ALGO == "RR"):
            
            slot_found = False
            while(not slot_found):

                max_slot_worker = 0
                cur_workers = WORKER_AVAILABILITY.keys()
                cur_workers.sort()

                for wid in cur_workers:

                    if(WORKER_AVAILABILITY[wid]["slots"] > 0): 
                        slot_found=True
                        max_slot_worker=wid
                        break
                
                # If Slot if Found Then Send the request
                if(slot_found):
                    
                    # Decrease Slot availability by 1
                    WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1

                    # Send the Request
                    s=socket.socket()
                    s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
                    s.send(json.dumps(task_to_send))
                    JOBS.pop(0)
                    break
                    

                else:
                    print("No Slots Found. Sleeping for One Second")
                    time.sleep(1)
            


        # Least Loaded Schedueling
        else:
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
                    
                    # Decrease Slot availability by 1
                    WORKER_AVAILABILITY[max_slot_worker]["slots"] -= 1

                    # Send the Request
                    s=socket.socket()
                    s.connect(('localhost',int(WORKER_AVAILABILITY[max_slot_worker]["port"])))
                    s.send(json.dumps(task_to_send))
                    JOBS.pop(0)
                    break
                    

                else:
                    print("No Slots Found. Sleeping for One Second")
                    time.sleep(1)




# Reading the command line arguments
PATH_TO_CONFIG = sys.argv[1]
SCHEDUELING_ALGO = sys.argv[2]
JOBS = list()


# Reading the config.json
config_file = open(PATH_TO_CONFIG,)
configuration = json.load(config_file)

# Initialized Worker Slots Availability
WORKER_AVAILABILITY = dict()
for worker in configuration["workers"]:
    WORKER_AVAILABILITY[worker["worker_id"]] = {}
    WORKER_AVAILABILITY[worker["worker_id"]]["slots"] = worker["slots"]
    WORKER_AVAILABILITY[worker["worker_id"]]["port"] = worker["port"]


# Start the thread to listen to jobs
job_listening_thread = threading.Thread(target = listen_job_request)
job_listening_thread.start()

job_scheduling_thread = threading.Thread(target = send_job_to_worker)
job_scheduling_thread.start()

worker_updates_thread = threading.Thread(target = listen_worker_update)
worker_updates_thread.start()
