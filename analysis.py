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


# df1 = df.loc(df["JobId"]==i)
print("Average Job Exceution Time for Random Scheduling: ", sum(master_log_random.filter(["Time"]))/len(master_log_random.filter(["Time"])))
print("Average Job Exceution Time for RR Scheduling: ", sum(master_log_rr.filter(["Time"]))/len(master_log_rr.filter(["Time"])))
print("Average Job Exceution Time for LL Scheduling: ", sum(master_log_ll.filter(["Time"]))/len(master_log_ll.filter(["Time"])))