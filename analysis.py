import csv
import pandas as pd

jobs=set()
tasks=[]

master_log_random=pd.read_csv("Masterlogs_Random.csv")
master_log_rr=pd.read_csv("Masterlogs_RR.csv")
master_log_ll=pd.read_csv("Masterlogs_LL.csv")

worker1_rr=pd.read_csv("1_log_file_rr.csv")
worker2_rr=pd.read_csv("2_log_file_rr.csv")
worker3_rr=pd.read_csv("3_log_file_rr.csv")

worker1_ll=pd.read_csv("1_log_file_ll.csv")
worker2_ll=pd.read_csv("2_log_file_ll.csv")
worker3_ll=pd.read_csv("3_log_file_ll.csv")

worker1_Random=pd.read_csv("1_log_file_Random.csv")
worker2_Random=pd.read_csv("2_log_file_Random.csv")
worker3_Random=pd.read_csv("3_log_file_Random.csv")


# Avergae Job exectuion time
random_job = master_log_random["Time"].tolist()
print("Average Job Exceution Time for Random Scheduling: ", sum(random_job) / len(random_job))
random_job.sort()
if(len(random_job)>0):
    print("Median Job Exceution Time for Random Scheduling: ", random_job[int((1+len(random_job))/2)])

ll_job = master_log_ll["Time"].tolist()
print("Average Job Time for Least Loaded Scheduling: ", sum(ll_job) / len(ll_job))
ll_job.sort()
if(len(ll_job)>0):
    print("Median Job Exceution Time for Random Scheduling: ", ll_job[int((1+len(ll_job))/2)])


rr_job = master_log_rr["Time"].tolist()
print("Average Job Exceution Time for Round Robin Scheduling: ", sum(rr_job) / len(rr_job))
rr_job.sort()
if(len(rr_job)>0):
    print("Median Job Exceution Time for Random Scheduling: ", rr_job[int((1+len(rr_job))/2)])


# Avergae Execution time for tasks
random_task = []
random_task.extend(worker1_Random['time'].tolist())
random_task.extend(worker2_Random['time'].tolist())
random_task.extend(worker3_Random['time'].tolist())
print("Average Tasks Time for Random Scheduling : ", sum(random_task)/ len(random_task))
random_task.sort()
if(len(random_task)>0):
    print("Median Job Exceution Time for Random Scheduling: ", random_task[int((1+len(random_task))/2)])


ll_task = []
ll_task.extend(worker1_ll['time'].tolist())
ll_task.extend(worker2_ll['time'].tolist())
ll_task.extend(worker3_ll['time'].tolist())
print("Average Tasks Time for Least Loaded Scheduling : ", sum(ll_task)/ len(ll_task))
ll_task.sort()
if(len(ll_task)>0):
    print("Median Job Exceution Time for Random Scheduling: ", ll_task[int((1+len(ll_task))/2)])


rr_task = []
rr_task.extend(worker1_rr['time'].tolist())
rr_task.extend(worker2_rr['time'].tolist())
rr_task.extend(worker3_rr['time'].tolist())
print("Average Tasks Time for Round Robin Scheduling : ", sum(rr_task)/ len(rr_task))
rr_task.sort()
if(len(rr_task)>0):
    print("Median Job Exceution Time for Random Scheduling: ", rr_task[int((1+len(rr_task))/2)])

# import numpy as np
# import pandas as pd
# vect1=np.zeros(10)
# vect2=np.ones(10)
# df=pd.DataFrame({'col1':vect1,'col2':vect2})

# a= df['col1'].tolist()
# print(a)