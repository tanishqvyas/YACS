import json
import socket
import time
import sys
import random
import numpy
import threading

def listen_request():
    master_host='localhost'
    master_port=5000
    master=socket.socket(socket.AF_INET)
    master.bind((master_host,master_port))
    master.listen(1)
    while True:
        job,address=s.accept()
        i=job.recv(1024).decode('utf-8')