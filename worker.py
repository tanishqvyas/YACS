import time


def worker_func(id, slots, port):
    
    for i in range(20):
        print("Worker : ", id, " doing ", i)
        time.sleep(1)