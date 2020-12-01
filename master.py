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
    master=socket.socket(socket.AF_INET)
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
    pass

# Function to Schedule TASKS
def send_job_to_worker():
    
    global SCHEDUELING_ALGO
    global WORKER_AVAILABILITY
    global JOBS

    # Check if task available
    while len(JOBS)==0:
        print("No tasks left to schedule")
        time.sleep(1)
    
    # Extracting task
    task_to_send={"jobId":JOBS[0][2],"taskId":JOBS[0][0],"interval":JOBS[0][1]}

    # Random Schedueling
    if(SCHEDUELING_ALGO == "Random"):
        pass

    # Round Robin Schedueling
    elif(SCHEDUELING_ALGO == "RR"):
        pass

    # Least Loaded Schedueling
    else:
        slot_found = False
        max_slots = 0
        max_slot_worker = 0
        while(not slot_found):

            for wid, aval_slots in WORKER_AVAILABILITY.items():

                if(WORKER_AVAILABILITY[wid] > max_slots): 
                    
                    max_slots = WORKER_AVAILABILITY[wid]
                    max_slot_worker = wid
                    slot_found = True
            
            # If Slot if Found Then Send the request
            if(slot_found):
                
                # Decrease Slot availability by 1
                WORKER_AVAILABILITY[max_slot_worker] -= 1

                # Send the Request
                s=socket.socket()
                


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
    WORKER_AVAILABILITY[worker["worker_id"]] = worker["slots"]



# Start the thread to listen to jobs
job_listening_thread = threading.Thread(target = listen_job_request)
job_listening_thread.start()
job_scheduling_thread = threading.Thread(target = send_job_to_worker)
job_scheduling_thread.start()
