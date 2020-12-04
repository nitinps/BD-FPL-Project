import re
import sys
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
start={}
task_time={}
reducer={}
job_time={}
worker_tasks=['' for x in range(3)]
total_time=[0 for x in range(3)]
avg_time=[0 for x in range(3)]
median_time=[0 for x in range(3)]
ma=0
for i in range(1,4):
	task_time[i]={}
	f=open('log/worker'+str(i)+'.txt')

	start_pat="^(\d{2}-\d{2}-\d{4},\s\d{2}:\d{2}:\d{2}:\d{6})\s*Received\stask\s(.*?)\sof\sjob.*"
	end_pat="^(\d{2}-\d{2}-\d{4},\s\d{2}:\d{2}:\d{2}:\d{6})\s*Finish\sjob\s(\d+)\stask\s(.*)"
	for line in f:
		m=re.search(start_pat,line)
		n=re.search(end_pat,line)
		if m:
		
			start[m.group(2)]=m.group(1)
		if n:
			r=re.search('(\d+)_R(\d+)',n.group(3))
			if r:
				reducer[n.group(3)]=n.group(1)
				if ma>int(r.group(1)):
					ma=int(r.group(1))
					
			for key,value in start.items():
				if n.group(3)==key:
					task_time[i][key]=(datetime.strptime(n.group(1),'%d-%m-%Y, %H:%M:%S:%f')-datetime.strptime(value,'%d-%m-%Y, %H:%M:%S:%f')).total_seconds()
	f.close()
	

for key,value in task_time.items():
	sum_time=0
	sum_tasks=0
	med=[]
	for task,time in value.items():
		sum_time+=time
		sum_tasks+=1
		med.append(time)
	med.sort()
	middle=len(med)//2
	worker_tasks[key-1]='Worker '+str(key)+', tasks '+str(sum_tasks)
	total_time[key-1]=sum_time
	try:
		median_time[key-1]=(med[middle]+med[~middle])/2
		avg_time[key-1]=sum_time/len(value)
	except:
		pass
#print(total_time)
#print(worker_tasks)

#print(reducer)

f=open('log/master.txt')
for line in f:
	job_rec="^(\d{2}-\d{2}-\d{4},\s\d{2}:\d{2}:\d{2}:\d{6})\s*Job\srequest\srecieved\sjob_id:(\d+),\smap_tasks_ids:\['(.*?)'\],\sreduce_tasks_ids:\['(.*?)'\]"
	j=re.search(job_rec,line)
	if j:
		print('Job '+j.group(2)+' Task completion time')
		s=j.group(4).split(', ')
		if len(s)>1:
			s[0]=s[0][:-1]
			for k in range(1,len(s)-1):
				s[k]=s[k][1:-1]
			s[-1]=s[-1][1:]
			last_task=s[-1]
		else:
			last_task=s[-1]
			
		t=j.group(3).split(', ')
		if len(t)>1:
			t[0]=t[0][:-1]
			for k in range(1,len(t)-1):
				t[k]=t[k][1:-1]
			t[-1]=t[-1][1:]
				
		print(s,t)
		for i in t:
			for l in range(1,4):
				if i in task_time[l].keys():
					print(i,task_time[l][i])
		for i in s:
			for l in range(1,4):
				if i in task_time[l].keys():
					print(i,task_time[l][i])
					
		job_time[j.group(2)]=(datetime.strptime(reducer[last_task],'%d-%m-%Y, %H:%M:%S:%f')-datetime.strptime(j.group(1),'%d-%m-%Y, %H:%M:%S:%f')).total_seconds()
		print('Job '+j.group(2)+' completion time = '+str(job_time[j.group(2)]))
		print('\n\n')
f.close()


fig=plt.subplots(figsize=(12,8))

plt.bar(worker_tasks,total_time,color='b',width=0.4,edgecolor ='grey')
plt.ylabel('Total Time',fontweight='bold')
plt.xlabel('Worker with number of tasks',fontweight='bold')
plt.show()

plt.bar(worker_tasks,avg_time,color='g',width=0.4,edgecolor ='grey')
plt.ylabel('Average Time',fontweight='bold')
plt.xlabel('Worker with number of tasks',fontweight='bold')
plt.show()

plt.bar(worker_tasks,median_time,color='r',width=0.4,edgecolor ='grey')
plt.ylabel('Median Time',fontweight='bold')
plt.xlabel('Worker with number of tasks',fontweight='bold')
plt.show()



'''red_time=[0 for x in range(ma+1)]
for i in range(ma+1):	
	last_task=0	
	for key,value in reducer.items():
		p=re.search(str(i)+'_R(\d+)',key)
		if p:
			if last_task'''
			
