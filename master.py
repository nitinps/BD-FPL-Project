import random
import threading
from datetime import *
import json
import os
import sys
import operator
import socket
from time import *

no_slots={}
port_nos={}
map_tasks_tot={}
job_counts={}
red_tasks={}
task_q=[]
    
def scheduler():
	sch_algo=sys.argv[2]
	while 1:
		if len(task_q):
			for task in task_q:
				if sch_algo=="RANDOM": 
					while 1:
						w_id = random.randint(1, 3)
						if no_slots[w_id]>0:
							worker_id = w_id
							break
							
				elif sch_algo=="RR":
					flag = 0
					while 1:
						for w_id in range(1,4):
							if no_slots[w_id]>0:
								flag = 1
								worker_id = w_id
								break
						if flag:
							break
							
				elif sch_algo=="LL":
					while 1:
						slots=[]
						for w_id in range(0,3):
							slots.append(no_slots[w_id+1])
						if slots[0]==0 and slots[1]==0 and slots[2]==0:
							sleep(1)
							continue
						worker_id = (slots.index(max(slots)) + 1)
						break
						
				else:
					print("RANDOM or RR or LL should be mentioned as the second argument")
					sys.exit()
				
				port = port_nos[worker_id]
				task = task_q.pop(0)
				msg = json.dumps(task)
				print("Task",task['task_id'],"is being sent to worker",worker_id,"on port", port)
				
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
					s.connect(("localhost", port))
					s.send(msg.encode())
					f = open("log/master.txt", "a+")
					write_lock.acquire()
					f.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f")+"\tConnecting with Worker {0} on {1} with port {2}\n".format(worker_id,"localhost", port))
					write_lock.release()
					f.close()
				print("Task",task['task_id'],"sent to worker",worker_id,"on port", port)
				work_lock.acquire()
				no_slots[worker_id]-=1
				work_lock.release()
									
				f=open("log/master.txt", "a+")
				write_lock.acquire()
				f.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f")+"\tSent task {0} of job {1} to worker {2} for a duration of {3}\n".format(task['task_id'], task['job_id'], worker_id, task['duration']))
				write_lock.release()
				f.close()
				

def get_requests():
	localhost = ""
	port = 5000
    
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.bind((localhost, port))
		s.listen(1000)
		while 1:
		
			conn,addr = s.accept()
			
			f = open("log/master.txt", "a+")
			write_lock.acquire()
			f.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f")+"\tConnection incoming from a job request from {0} using port {1}\n".format(addr[0], addr[1]))
			write_lock.release()
			f.close()
			with conn:
				req=conn.recv(100000)
				requests=json.loads(req)
				mapTaskIds = []
				for i in requests['map_tasks']:
					mapTaskIds.append(i['task_id'])
				reduceTaskIds = []
				for i in requests['reduce_tasks']:
					reduceTaskIds.append(i['task_id'])
				f = open("log/master.txt", "a+")
				write_lock.acquire()
				f.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f")+"\tJob request recieved job_id:{0}, map_tasks_ids:{1}, reduce_tasks_ids:{2}\n".format(requests['job_id'], mapTaskIds, reduceTaskIds))
				write_lock.release()
				f.close()
				no_map_tasks = len(requests['map_tasks'])
				map_tasks_tot[requests['job_id']] = no_map_tasks #number of map tasks
				
                
				job_counts[requests['job_id']]=no_map_tasks+ len(requests['reduce_tasks'])

				red_task = {}
				red_tasks[requests['job_id']] = []
				tasks = requests['reduce_tasks']

				for t in tasks:
					red_task[t['task_id']] = {'task_id': t['task_id'], 'job_id': requests['job_id'], 'duration': t['duration']}
					red_tasks[requests['job_id']].append(red_task[t['task_id']])
				
				map_task = {}	                
				tasks = requests['map_tasks']
				
				for t in tasks:
					map_task[t['task_id']] = {'task_id': t['task_id'], 'job_id': requests['job_id'], 'duration': t['duration']}
					task_q.append(map_task[t['task_id']])
                

def get_updates():
	localhost=""
	port = 5001
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.bind((localhost, port))
		s.listen(1000)
		while 1:

			conn,addr=s.accept()

			f= open("log/master.txt", "a+")
			write_lock.acquire()
			f.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f")+"\tConnection incoming from a worker from {0} using port {1}\n".format(addr[0], addr[1]))
			write_lock.release()
			f.close()
			with conn:
				update=conn.recv(1000)
				update_json=json.loads(update)
				worker_id=update_json['worker_id']
				task_id=update_json['task_id']
				job_id=update_json['job_id']

			print("Task",task_id,"completed execution on worker",worker_id)
			f = open("log/master.txt", "a+")
			write_lock.acquire()
			f.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f")+"\tUpdate from worker {0} task {1} completed\n".format(worker_id, task_id))
			write_lock.release()
			f.close()
			
			work_lock.acquire()
			no_slots[worker_id]+=1
			work_lock.release()
			
			job_counts[job_id]-=1
			if job_counts[job_id]==0:
				f=open("log/master.txt", "a+")
				write_lock.acquire()
				f.write(datetime.now().strftime("%d-%m-%Y, %H:%M:%S:%f")+"\tFinished execution of job {0}\n".format(job_id))
				write_lock.release()
				f.close()
			
			if 'M' in task_id: #map job is completed
				map_tasks_tot[job_id]-=1
				if map_tasks_tot[job_id]==0:
					for reduce_task in red_tasks[job_id]:
						task_q.append(reduce_task)
                        

work_lock=threading.Lock()
write_lock=threading.Lock()

try:
	os.mkdir('log')
except:
	pass 
f=open("log/master.txt", "w")
f.close()
path_to_config = sys.argv[1]
config=open(path_to_config)
conf_json= json.load(config)
config.close()
for worker in conf_json['workers']:
	no_slots[worker['worker_id']]=worker['slots']
	port_nos[worker['worker_id']]=worker['port']

req_thread = threading.Thread(target=get_requests)
updates_thread = threading.Thread(target=get_updates)
sch_thread = threading.Thread(target=scheduler)

req_thread.start()
updates_thread.start()
sch_thread.start()

req_thread.join()
updates_thread.join()
sch_thread.join()  

