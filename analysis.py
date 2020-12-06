import csv
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

'''
This file is used to analyse the log files created by YACS.
It compares the mean and median execution times for jobs and tasks
This is done for each of the Scheduling Algorithms - RR, LL and Random
'''

jobs=set()
tasks=[]

# Reading all the created log files in multiple runs with various scheduling algorithms

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


# Calculating Avergae Job Execution Time for Random Scheduling
random_job = master_log_random["Time"].tolist()

print("Average Job Execution Time for Random Scheduling: ", sum(random_job) / len(random_job))
random_job.sort()

# Calculating Median of the Job Execution Times for Random Scheduling
if(len(random_job)>0):
    if(len(random_job)%2 == 0):
        median_random = (random_job[int((len(random_job))/2)] + random_job[int((len(random_job)+2)/2)])/2
    else:
        median_random = random_job[int((1+len(random_job))/2)]

    print("Median Job Execution Time for Random Scheduling: ", median_random)

# Calculating Average Job Execution Time for Least Loaded Scheduling
ll_job = master_log_ll["Time"].tolist()
print("Average Job Execution Time for Least Loaded Scheduling: ", sum(ll_job) / len(ll_job))
ll_job.sort()

# Calculating Median of the Job Execution Times for Least Loaded Scheduling
if(len(ll_job)>0):
    if(len(ll_job)%2 == 0):
        median_ll = (ll_job[int((len(ll_job))/2)] + ll_job[int((len(ll_job)+2)/2)])/2
    else:
        median_ll = ll_job[int((1+len(ll_job))/2)]

    print("Median Job Execution Time for Least Loaded Scheduling: ", ll_job[int((1+len(ll_job))/2)])


rr_job = master_log_rr["Time"].tolist()
print("Average Job Execution Time for Round Robin Scheduling: ", sum(rr_job) / len(rr_job))
rr_job.sort()

# Calculating Median of the Job Execution Times for Round Robin Scheduling
if(len(rr_job)>0):
    if(len(rr_job)%2 == 0):
        median_rr = (rr_job[int((len(rr_job))/2)] + rr_job[int((len(rr_job)+2)/2)])/2
    else:
        median_rr = rr_job[int((1+len(rr_job))/2)]
    print("Median Job Execution Time for Round Robin Scheduling: ", rr_job[int((1+len(rr_job))/2)])


# Calculating Average Task Execution Time for Random Scheduling
random_task = []
random_task.extend(worker1_Random['time'].tolist())
random_task.extend(worker2_Random['time'].tolist())
random_task.extend(worker3_Random['time'].tolist())
print("Average Tasks Time for Random Scheduling : ", sum(random_task)/ len(random_task))
random_task.sort()

# Calculating Median of the Task Execution Times for Random Scheduling
if(len(random_task)>0):
    if(len(random_task)%2 == 0):
        t_median_random = (random_task[int((len(random_task))/2)] + random_task[int((len(random_task)+2)/2)])/2
    else:
        t_median_random = random_task[int((1+len(random_task))/2)]
    print("Median Tasks Execution Time for Random Scheduling: ", t_median_random)

# Calculating Average Task Execution Time for Least Loaded Scheduling
ll_task = []
ll_task.extend(worker1_ll['time'].tolist())
ll_task.extend(worker2_ll['time'].tolist())
ll_task.extend(worker3_ll['time'].tolist())
print("Average Tasks Time for Least Loaded Scheduling : ", sum(ll_task)/ len(ll_task))
ll_task.sort()

# Calculating Median of the Task Execution Times for Least Loaded Scheduling
if(len(ll_task)>0):
    if(len(ll_task)%2 == 0):
        t_median_ll = (ll_task[int((len(ll_task))/2)] + ll_task[int((len(ll_task)+2)/2)])/2
    else:
        t_median_ll = ll_task[int((1+len(ll_task))/2)]
    print("Median Tasks Execution Time for Least Loaded Scheduling: ", t_median_ll)


# Calculating Average Task Execution Time for Round Robin Scheduling
rr_task = []
rr_task.extend(worker1_rr['time'].tolist())
rr_task.extend(worker2_rr['time'].tolist())
rr_task.extend(worker3_rr['time'].tolist())
print("Average Tasks Time for Round Robin Scheduling : ", sum(rr_task)/ len(rr_task))
rr_task.sort()

# Calculating Median of the Task Execution Times for Round Robin Scheduling
if(len(rr_task)>0):
    if(len(rr_task)%2 == 0):
        t_median_rr = (rr_task[int((len(rr_task))/2)] + rr_task[int((len(rr_task)+2)/2)])/2
    else:
        t_median_rr = rr_task[int((1+len(rr_task))/2)]
    print("Median Tasks Execution Time for Round Robin Scheduling: ", t_median_rr)

#Plotting GRAPHS

labels = ['Random scheduling','Least loaded', 'Round robin']

means = [sum(random_job) / len(random_job),sum(ll_job) / len(ll_job), sum(rr_job) / len(rr_job) ]
medians = [median_random,median_ll,median_rr]

x = np.arange(len(labels))  # the label locations
width = 0.35                # the width of the bar

fig, ax = plt.subplots()
bars1 = ax.bar(x - width/2, means, width, label='Mean')
bars2 = ax.bar(x + width/2, medians, width, label='Median')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Time')
ax.set_title('Mean and median completion time for Jobs vs Scheduling algorithm')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()


def value_of_bar(bars):
    # Attaches label above each bar in the graph
    # The label represents the height of the graph
    for rect in bars:
        height = round(rect.get_height(),3)
        print("Height",height)
        ax.annotate('{}'.format(height),xy=(rect.get_x() + rect.get_width() / 2, height),xytext=(0, 3),textcoords="offset points",ha='center', va='bottom')


value_of_bar(bars1)
value_of_bar(bars2)

fig.tight_layout()
plt.show()

# Mean and Median time taken for task Vs Scheduling algorithm
means = [sum(random_task) / len(random_task),sum(ll_task) / len(ll_task), sum(rr_task) / len(rr_task) ]
medians = [t_median_random,t_median_ll,t_median_rr]


x = np.arange(len(labels))  # the label locations
width = 0.35                # the width of the bar

fig, ax = plt.subplots()
bars1 = ax.bar(x - width/2, means, width, label='Mean')
bars2 = ax.bar(x + width/2, medians, width, label='Median')

# Adding titles and labels
ax.set_ylabel('Time')
ax.set_title('Mean and median completion time for Tasks vs Scheduling algorithm')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()

value_of_bar(bars1)
value_of_bar(bars2)
fig.tight_layout()

# VISUAL REPRESENTATION OF PART 2 RESULT 1
plt.show()


# REPRESENTING PART 2 RESULT 2
# Plots the	number of tasks	scheduled on each machine, against time
# for each scheduling algorithm	

def task_plot(file_name):
    # Opening the log files with times at which tasks were sent and received
    to_plot=open("tasks_Masterlogs_"+file_name+".csv").read().split("\n")
    workers=dict()
    timestamps=dict()
    for line in to_plot[1:]:
        if line:
            wid,tasks,time=line.split(",")
            tasks,time=int(tasks),float(time)
            try:
                workers[wid].append(tasks)
                timestamps[wid].append(time)
            except:
                workers[wid]=[tasks]
                timestamps[wid]=[time]
    fig, ax = plt.subplots(figsize=(25, 5))
    ax.plot(timestamps["1"],workers["1"], 'g', label="Worker 1")
    ax.plot(timestamps["2"],workers["2"], 'r', label="Worker 2")
    ax.plot(timestamps["3"],workers["3"], 'b', label="Worker 3")
    ax.set_title('Number of Tasks vs Time in '+file_name+ " Scheduling")
    ax.set_xlabel('Time')
    ax.set_ylabel('No of tasks in the Worker')
    legend = ax.legend(loc='best')
    plt.show()
# Creating the plots for all 3 scheduling algorithms
task_plot("Random")
task_plot("RR")
task_plot("LL")