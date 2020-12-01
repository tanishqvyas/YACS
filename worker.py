import socket

def listen_for_master(port):
  s = socket.socket()            
  s.bind(('', port))        
  s.listen(5)               
  while True:  
      c, addr = s.accept()       
      r_json = c.recv(1024)
      task = json.loads(recv_json)
      t2 = threading.Thread(target=update_master, args=(task_desc)) 
      t2.start()
      c.close()

def update_master(task):
    pass