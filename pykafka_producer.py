import sys
from pykafka import KafkaClient
from pykafka.partitioners import HashingPartitioner
from pykafka.partitioners import BasePartitioner
import numpy as np
import time


hash_partitioner = HashingPartitioner()


#client   = KafkaClient("172.31.53.147:9092") # , socket_timeout_ms=1000
client   = KafkaClient(hosts="172.31.55.173:9092,172.31.53.162:9092,172.31.56.220:9092",zookeeper_hosts="172.31.53.147:2181")
topic    = client.topics["jtest30b"]
#producer = topic.get_producer(partitioner=hash_partitioner, linger_ms = 200)

totallogs = 0
times = time.time()
"""
with topic.get_producer(partitioner=hash_partitioner, linger_ms = 200) as producer:
	while True:
		tw = 1*60. # The time window in seconds
		#visitg = [1,2,10,100]
		visitg = [1, 2, 10, 1000] 
		nvfv = {'1'   :[0       , int(1e6), int(1e3)], # [0,   1e6): (forgetters)
			'2'   :[int(1e6), int(2e6), int(1e3)], # [1e6, 2e6): (login-logout) 
			'10'  :[int(2e6), int(3e6), int(1e3)], # [2e6, 3e6): (active-user)
			'1000' :[int(3e6), int(4e6), int(40000)]} # [3e6, 4e6): (machine-spam)
		dtime = 0.
		nv = 0 # Initiate the number of the visits to the website during the time window
		ld = []
		for v in visitg:
			nv += v*nvfv[str(v)][2]
			ld.append(np.repeat(np.random.randint(nvfv[str(v)][0], nvfv[str(v)][1], size=nvfv[str(v)][2]), v)) 

		ld1 = np.concatenate(ld[:])
		np.random.shuffle(ld1)
		dt = tw/len(ld1)
		eventTime = 0.

		for index, item in enumerate(ld1):
			eventTimeo = eventTime  
			eventTime  = dt*(index+np.random.random_sample()) # event time in Seconds
			time.sleep((eventTime-eventTimeo)) # sleep time in miliseconds
			currID = item
			#outputStr = "%s;%s" % (currID, eventTime+dtime)
                        outputStr = "%s;%s" % (currID, np.int64(1))

			producer.produce(outputStr, partition_key=str(currID))
		dtime += tw
		totallogs += len(ld1)

"""
ld1 = []
for i in range(30):
	ld1.append([i,1]) 
with topic.get_producer(partitioner=hash_partitioner, linger_ms = 200) as producer:
	for index, item in enumerate(ld1):
		outputStr = "%s;%s" % (item[0], item[1])
		producer.produce(outputStr, partition_key=str(item[0]))




print time.time()-times

