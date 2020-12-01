import json
import socket
import time
import sys
import random
import numpy
import threading

jobs=[] # in case more than one job is submitted
def listen_job_request():
    global jobs
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

        jobs[jobId] = [list(),list(),time.time()]   #maps,reduces

        for m in requests['map_tasks']:
            jobs[jobId][0].append([m['task_id'], m['duration']])
        for r in requests['reduce_tasks']:
            jobs[jobId][1].append([r['task_id'], r['duration']])

        job.close()

def listen_worker_update():
    pass
def send_job_to_worker():
    pass

