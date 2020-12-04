import sys
import json
import threading
from socket import *
from time import *
from datetime import *
import time
import random
import os

worker_id = sys.argv[2]
		
def initialize_worker(worker_id, port):
	worker_list = {"worker_id": worker_id, "free_slots": 0, "slots": 0, "port": port, "execution_list": [] }
	if worker_id == 1:
		worker_list["slots"] = 5
		worker_list["free_slots"] = 5
		for i in range(5):
			worker_list["execution_list"].append(0)
	elif worker_id==2:
		worker_list["slots"] = 7
		worker_list["free_slots"] = 7
		for i in range(7):
			worker_list["execution_list"].append(0)
	else:
		worker_list["slots"] = 3
		worker_list["free_slots"] = 3
		for i in range(3):
			worker_list["execution_list"].append(0)
			
	worker_file=open("log/worker"+str(worker_id)+".txt", "a+")
	write_lock.acquire()
	worker_file.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f") + "\tWorker {0} has started on port {1} with {2} slots\n".format(worker_list["worker_id"], worker_list["port"], worker_list["slots"]))
	write_lock.release()
	worker_file.close()
	return worker_list
		
def start_task(worker_list, task_dict):
	task = {"job_id": task_dict["job_id"], "task_id": task_dict["task_id"], "remaining_time": task_dict["duration"]}
	worker_file = open("log/worker"+str(worker_list["worker_id"])+".txt", "a+")
	write_lock.acquire()
	worker_file.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f") + "\tStart job {0} task {1}\n".format(task_dict["job_id"], task_dict["task_id"]))
	write_lock.release()
	worker_file.close()
	for i in range(len(worker_list["execution_list"])):
		if (worker_list["execution_list"][i] == 0): 
			worker_list["execution_list"][i] = task
			worker_list["free_slots"]-= 1
			break
			

def getStartMsg(worker_list):
	with socket(AF_INET, SOCK_STREAM) as s:
		s.bind(("localhost", worker_list["port"]))
		s.listen(1024)
		while 1:
			conn, addr=s.accept()
			worker_file = open("log/worker"+str(worker_list["worker_id"])+".txt", "a+")
			write_lock.acquire()
			worker_file.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f")+"\tConnection incoming from a master from {0} using port {1}\n".format(addr[0], addr[1]))
			write_lock.release()
			worker_file.close()
			with conn:
				msg = conn.recv(1024).decode()
				if msg:
					task=json.loads(msg)
					worker_file=open("log/worker"+str(worker_list["worker_id"])+".txt", "a+")
					write_lock.acquire()
					worker_file.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f")+"\tReceived task {0} of job {1} for a duration of {2}]\n".format(task["task_id"], task["job_id"],  task["duration"]))
					write_lock.release()
					worker_file.close()
					work_lock.acquire()
					start_task(worker_list,task)
					work_lock.release()
                
def executeTasks(worker_list):
	while 1:
		for i in range(worker_list["slots"]):
			if ( worker_list["execution_list"][i]==0):
				continue
			elif (worker_list["execution_list"][i]["remaining_time"]==0):
				task = worker_list["execution_list"][i]
				task_id = task["task_id"]
				job_id = task["job_id"]
				msg = {"worker_id": worker_list["worker_id"], "job_id": job_id, "task_id": task_id}
				worker_file = open("log/worker"+str(worker_list["worker_id"])+".txt", "a+")
				write_lock.acquire()
				worker_file.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f") + "\tFinish job {0} task {1}\n".format(job_id, task_id))
				write_lock.release()
				worker_file.close()
				work_lock.acquire()
				worker_list["execution_list"][i]=0
				worker_list["free_slots"]+=1	
				work_lock.release()
				
				msg_json=json.dumps(msg)
				with socket(AF_INET, SOCK_STREAM) as s:
					s.connect(('localhost', 5001))
					s.send(msg_json.encode())
					worker_file=open("log/worker"+str(worker_list["worker_id"])+".txt", "a+")
					write_lock.acquire()
					worker_file.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f") + "\tConnecting with Master on {0} with port {1}\n".format("localhost", "5001"))
					write_lock.release()
					worker_file.close()
				worker_file=open("log/worker"+str(worker_list["worker_id"])+".txt", "a+")
				write_lock.acquire()
				worker_file.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f") + "\tUpdate sent to master job {0} task_id {1} completed\n".format(job_id, task_id))
				write_lock.release()
				worker_file.close()
			else:
				work_lock.acquire()
				worker_list["execution_list"][i]["remaining_time"]-=1
				work_lock.release()
		sleep(1)
  
work_lock=threading.Lock()
write_lock=threading.Lock() #to lock printing to log files          


try:
	os.mkdir('log')
except:
	pass #dir already exists
if (len(sys.argv)!=3):
	print("Error")
	sys.exit(-1)
f=open("log/worker"+sys.argv[2]+".txt", "w")
f.close()
port = int(sys.argv[1])
worker_id = int(sys.argv[2])
worker_list = initialize_worker(worker_id, port)
    
get_task_thread = threading.Thread(target=getStartMsg, args=(worker_list,))
execute_thread = threading.Thread(target=executeTasks, args=(worker_list,))
get_task_thread.start()
execute_thread.start()
get_task_thread.join()
execute_thread.join()

