# Producer-Consumer Code
# Written by Matt DiPaolo

import threading
import queue
import random
import time

MIN_SIZE= 1         # min buffer size
MAX_SIZE = 5        # max buffer size
NUM_QUEUES = 3      # numbers of consumers
entry = 0           # used for data production
queues = []         # 2d array to hold the buffer, the mutex lock, and mutex lock
RUN_TIME = 20       # seconds to run the simulation for

#consumes from the specified buffer - DONE
def consume(buffer):
    global queues
    while (True):
        queues[buffer][1].acquire()    
        if not queues[buffer][0].empty():
            print("CONSUMER: "+str(queues[buffer][0].get())+" was consumed from Queue "+str(buffer))
        queues[buffer][1].release()
        time.sleep(random.random())
        
# adds to the shortest buffer
def produce():
    global entry
    global queues
    while (True):
        buffer = findAndLockShortest() 
        if (not buffer[0].full()):
            entry+=1
            buffer[0].put(entry)
            print("PRODUCER: Added "+str(entry)+" to Queue "+str(buffer[3]))
        buffer[1].release()
        
#returns the shortest buffer, will randomize if there are equal size buffers
# acquires the chosen buffer
def findAndLockShortest():
    global queues
    sameSize = []
    shortest = MAX_SIZE+1
    for q in queues:
        q[1].acquire()
        if (q[0].qsize() < shortest):
            sameSize = [q]
            shortest = q[0].qsize()
        elif (q[0].qsize() == shortest):
            sameSize.append(q)
    # released the non chosen queues
    choice = random.choice(sameSize)
    for q in queues:
        if not (q == choice):
            q[1].release()
    return choice


#________________MAIN PROGRAM___________________#

# populate queues(2-d array). Each 1d index holds a buffer of random length, 
# the mutex lock, and the thread for each consumer
for i in range(NUM_QUEUES):
    queues.append(
        [queue.Queue(maxsize = random.randint(MIN_SIZE,MAX_SIZE)), # 2d index 0 references the buffer
        threading.Condition(),                                     # 2d index 1 references the lock
        threading.Thread(target=consume,args=(i,),daemon = True),  # 2d index 2 references the thread
        i]                                                         # 2d index 3 references the queue number
    )                                          
# create the producer thread
producerThread = threading.Thread(target=produce,daemon = True)
#print the queue capacities
for i in range(NUM_QUEUES):
    print("Queue "+str(i)+" Capacity: "+str(queues[i][0].maxsize))
#starting all threads
for q in queues:
    q[2].start()
producerThread.start()

time.sleep(RUN_TIME)
for q in queues:
    q[1].acquire()
print("Done")
