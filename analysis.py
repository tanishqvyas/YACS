import csv
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

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

#Plotting GRAPHS

labels = ['Random scheduling','Least loaded', 'Round robin']

means = [sum(random_job) / len(random_job),sum(ll_job) / len(ll_job), sum(rr_job) / len(rr_job) ]
medians = [random_job[int((1+len(random_job))/2)], ll_job[int((1+len(ll_job))/2)], rr_job[int((1+len(rr_job))/2)]]

x = np.arange(len(labels))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, means, width, label='Mean')
rects2 = ax.bar(x + width/2, medians, width, label='Median')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Time')
ax.set_title('Mean and median completion time for Jobs vs Scheduling algorithm')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()


def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = round(rect.get_height(),3)
        print("Height",height)
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')


autolabel(rects1)
autolabel(rects2)

fig.tight_layout()
plt.show()

# Mean and Median time taken for task Vs Scheduling algorithm
means = [sum(random_task) / len(random_task),sum(ll_task) / len(ll_task), sum(rr_task) / len(rr_task) ]
medians = [random_task[int((1+len(random_task))/2)], ll_task[int((1+len(ll_task))/2)], rr_task[int((1+len(rr_task))/2)]]


x = np.arange(len(labels))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, means, width, label='Mean')
rects2 = ax.bar(x + width/2, medians, width, label='Median')

# Add some text for labels, title and custom x-axis tick labels, etc.
#fig, ax = plt.subplots()
ax.set_ylabel('Time')
ax.set_title('Mean and median completion time for Tasks vs Scheduling algorithm')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()

autolabel(rects1)
autolabel(rects2)
fig.tight_layout()

plt.show()


        