import json
import socket
import time
import sys
import random
import numpy
import threading

#---------- Custom Imports -------------#
#from worker import worker_func

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

        JOBS[jobId] = [list(),list(),time.time()]   #maps,reduces

        for m in requests['map_tasks']:
            JOBS[jobId][0].append([m['task_id'], m['duration']])
        for r in requests['reduce_tasks']:
            [jobId][1].append([r['task_id'], r['duration']])

        job.close()

def listen_worker_update():
    pass
def send_job_to_worker():
    pass

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
    WORKER_AVAILABILITY[worker["worker_id"]]["available slots"] = worker["slots"]


# Threads creation for workers
'''
# WORKER THREADS
WORKER_THREADS = dict()

## Starting the worker threads as per the configuration
for worker in configuration["workers"]:

    # Initialize worker Thread and pass the appropriate worker_id, number_of_slots, port
    WORKER_THREADS[worker["worker_id"]] = threading.Thread(target=worker, args=(worker["worker_id"], worker["slots"], worker["port"]))
    print("(status= active) Worker ", worker["worker_id"], " has ", worker["slots"] ," slots.\n")
    WORKER_THREADS[worker["worker_id"]].start()
'''

t1 = threading.Thread(target = listen_job_request)
t1.start()
t1.join()