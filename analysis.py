import csv
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

jobs=set()
tasks=[]

master_log_random=pd.read_csv("Masterlogs_Random.csv")
master_log_rr=pd.read_csv("Masterlogs_RR.csv")
master_log_ll=pd.read_csv("Masterlogs_LL.csv")

worker1_rr=pd.read_csv("1_log_file_RR.csv")
worker2_rr=pd.read_csv("2_log_file_RR.csv")
worker3_rr=pd.read_csv("3_log_file_RR.csv")

worker1_ll=pd.read_csv("1_log_file_LL.csv")
worker2_ll=pd.read_csv("2_log_file_LL.csv")
worker3_ll=pd.read_csv("3_log_file_LL.csv")

worker1_Random=pd.read_csv("1_log_file_Random.csv")
worker2_Random=pd.read_csv("2_log_file_Random.csv")
worker3_Random=pd.read_csv("3_log_file_Random.csv")


# Avergae Job exectuion time
random_job = master_log_random["Time"].tolist()

print("Average Job Exceution Time for Random Scheduling: ", sum(random_job) / len(random_job))
random_job.sort()
if(len(random_job)>0):
    if(len(random_job)%2 == 0):
        median_random = (random_job[int((len(random_job))/2)] + random_job[int((len(random_job)+2)/2)])/2
    else:
        median_random = random_job[int((1+len(random_job))/2)]

        print("Median Job Exceution Time for Random Scheduling: ", median_random)

ll_job = master_log_ll["Time"].tolist()
print("Average Job Time for Least Loaded Scheduling: ", sum(ll_job) / len(ll_job))
ll_job.sort()
if(len(ll_job)>0):
    if(len(ll_job)%2 == 0):
        median_ll = (ll_job[int((len(ll_job))/2)] + ll_job[int((len(ll_job)+2)/2)])/2
    else:
        median_ll = ll_job[int((1+len(ll_job))/2)]

    print("Median Job Exceution Time for Least Loaded Scheduling: ", ll_job[int((1+len(ll_job))/2)])


rr_job = master_log_rr["Time"].tolist()
print("Average Job Exceution Time for Round Robin Scheduling: ", sum(rr_job) / len(rr_job))
rr_job.sort()
if(len(rr_job)>0):
    if(len(rr_job)%2 == 0):
        median_rr = (rr_job[int((len(rr_job))/2)] + rr_job[int((len(rr_job)+2)/2)])/2
    else:
        median_rr = rr_job[int((1+len(rr_job))/2)]
    print("Median Job Exceution Time for Round Robin Scheduling: ", rr_job[int((1+len(rr_job))/2)])


# Avergae Execution time for tasks
random_task = []
random_task.extend(worker1_Random['time'].tolist())
random_task.extend(worker2_Random['time'].tolist())
random_task.extend(worker3_Random['time'].tolist())
print("Average Tasks Time for Random Scheduling : ", sum(random_task)/ len(random_task))
random_task.sort()
if(len(random_task)>0):
    if(len(random_task)%2 == 0):
        t_median_random = (random_task[int((len(random_task))/2)] + random_task[int((len(random_task)+2)/2)])/2
    else:
        t_median_random = random_task[int((1+len(random_task))/2)]
    print("Median Job Exceution Time for Random Scheduling: ", t_median_random)


ll_task = []
ll_task.extend(worker1_ll['time'].tolist())
ll_task.extend(worker2_ll['time'].tolist())
ll_task.extend(worker3_ll['time'].tolist())
print("Average Tasks Time for Least Loaded Scheduling : ", sum(ll_task)/ len(ll_task))
ll_task.sort()

if(len(ll_task)>0):
    if(len(ll_task)%2 == 0):
        t_median_ll = (ll_task[int((len(ll_task))/2)] + ll_task[int((len(ll_task)+2)/2)])/2
    else:
        t_median_ll = ll_task[int((1+len(ll_task))/2)]
    print("Median Job Exceution Time for Least Loaded Scheduling: ", t_median_ll)


rr_task = []
rr_task.extend(worker1_rr['time'].tolist())
rr_task.extend(worker2_rr['time'].tolist())
rr_task.extend(worker3_rr['time'].tolist())
print("Average Tasks Time for Round Robin Scheduling : ", sum(rr_task)/ len(rr_task))
rr_task.sort()
if(len(rr_task)>0):
    if(len(rr_task)%2 == 0):
        t_median_rr = (rr_task[int((len(rr_task))/2)] + rr_task[int((len(rr_task)+2)/2)])/2
    else:
        t_median_rr = rr_task[int((1+len(rr_task))/2)]
    print("Median Job Exceution Time for Round Robin Scheduling: ", t_median_rr)

#Plotting GRAPHS

labels = ['Random scheduling','Least loaded', 'Round robin']

means = [sum(random_job) / len(random_job),sum(ll_job) / len(ll_job), sum(rr_job) / len(rr_job) ]
medians = [median_random,median_ll,median_rr]

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
medians = [t_median_random,t_median_ll,t_median_rr]


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


def task_plot(algo):
    x_axis = dict()
    y_axis = dict()
    numberOfTasks=dict()
    for worker_id in [1,2,3]:
        #graph[worker_id]=[]
        lines=open(str(worker_id)+"_task_log_file_"+algo+".csv").read().split("\n")
        x_axis[worker_id]=[0]
        y_axis[worker_id]=[0]
        points=[(0,0)]
        numberOfTasks[worker_id]=0
        
        for line in lines[1:]:
            if line:
                print(line)
                values = line.split(",")
                message=values[1].strip()
                time = values[2]
                if message == "TASK BEGUN":
                    task_id = values[0]
                    numberOfTasks[worker_id] += 1
                    #x_axis[worker_id].append(time)
                    #y_axis[worker_id].append(numberOfTasks[worker_id])
                    points.append((time,numberOfTasks[worker_id]))
                if message == "TASK END":
                    numberOfTasks[worker_id] -= 1
                    #x_axis[worker_id].append(time)
                    #y_axis[worker_id].append(numberOfTasks[worker_id])
                    points.append((time,numberOfTasks[worker_id]))
        points.sort(key=lambda x:x[1])
        x_axis[worker_id]=[i[0] for i in points]
        y_axis[worker_id]=[i[1] for i in points]
    fig, ax = plt.subplots(figsize=(25, 5))
    ax.plot(x_axis[1], y_axis[1], 'g', label="Worker 1")
    ax.plot(x_axis[2], y_axis[2], 'r', label="Worker 2")
    ax.plot(x_axis[3], y_axis[3], 'b', label="Worker 3")
    ax.set_xlabel('Time')
    ax.set_ylabel('No of tasks in the Worker')
    legend = ax.legend(loc='best')
    plt.show()

for i in ["Random","RR","LL"]:
    task_plot(i)