import xmlrpclib
import os
import hashlib
import time
import sys
import traceback
import random
import socket
import threading
import copy
import pickle
import signal
import os.path
import subprocess
import random
from collections import deque
import Queue
from weasel.utils.notification import *
import weasel.etc.config as config
from weasel.utils.logger import WeaselLogger


class Scheduler(object):
    stdin = "/dev/null"
    stdout = "/dev/null"
    stderr = "/dev/null"

    def __init__(self):
        self.running = True
        self.resource_queues = {'cpu': [],
                                'memory': [],
                                'bw': []}
        ''' this is to have multiple applications/queues; application = queue '''
        self.task_queue = []  # ready queues; [queue_id]
        self.task_queue_data = {}  # ready queue data; queue_id: [task...]
        # queues with pending tasks, which do not have the input ready
        self.pending_queue_data = {}
        self.task_queue_lock = threading.Lock()
        self.pending_task_queue_lock = threading.Lock()
        logfile = config.LOGDIR + "/scheduler.log"
        taskfile = config.LOGDIR + "/task.log"
        self.logger = WeaselLogger('scheduler', logfile)
        self.task_logger = WeaselLogger('tasklogger', logfile)
        ''' this is the thread that will perform the communication with the local schedulers '''
        self.server_thread = ZmqConnectionThread(
            'resourcemng',
            zmq.ROUTER,
            "*:" + str(
                config.ZMQ_SCHEDULER_PORT),
            self.msg_process_callback)
        ''' this information is for keeping track of files and task dependencies '''
        self.task_to_file = {
        }  # taskid: {'nfiles': 0, 'ntotalfiles':x, 'outputs':[files]}
        # file: {'ntids': ntids, 'tids': [tids], 'ctids': executed_tasks}
        self.file_to_task = {}
        self.result_queue = Queue()
        self.result_consumer_thread = threading.Thread(target=self.get_result)
        self.result_consumer_thread.start()
        ''' idle worker information and client notification '''
        self.waiting_clients = []
        self.workers_empty_dict = {}
        self.workers = []
        self.workers_data = {}
	self.workers_lock = threading.Lock()
        self.workers_empty = 0
        self.task_id = 0
        ''' to notify workers about new queues '''
        self.new_queues = []
        self.new_queues_lock = threading.Lock()
        ''' here I have: the thread that listens for messages
            a queue in which the tasks are put
            the main thread that applies some reconfiguration (?)
        '''
        self.files_to_delete = []

    def delete_queue(self, qid):
        del self.task_queue_data[qid]
        self.task_queue.remove(qid)
        self.new_queues_lock.acquire()
        try:
            self.new_queues.remove(qid)
        except:
            pass
        self.new_queues_lock.release()
        
        
    def get_taskids(self):
        tasks_ids = []
        try:
            tasks_ids = self.result_queue.get(
                    block=True,
                    timeout=8 *
                    config.WAITTIME)
        except:
            self.pending_task_queue_lock.acquire()
            pending_queue = self.pending_queue_data
            for pqueue in pending_queue:
                to_delete = []
                for tid in pending_queue[pqueue]:
                    current_inputs = 0
                    tinputs = len(pending_queue[pqueue][tid]['inputs'])
                    for inputf in pending_queue[pqueue][tid]['inputs']:
                        if os.path.isfile(inputf):
                            current_inputs = current_inputs + 1
                        #else:
                        #    print "Missing file ", inputf
                    if current_inputs == tinputs:
                        task = pending_queue[pqueue][tid]
                        data = {
                                'id': tid,
                                'exec': task['exec'],
                                'params': task['params']}
                        to_delete.append(tid)
                        self.task_queue_lock.acquire()
                        is_new = False
                        try:
                            self.task_queue_data[pqueue].append(data)
                        except:
                            self.task_queue.append(pqueue)
                            self.task_queue_data[pqueue] = deque()
                            self.task_queue_data[pqueue].append(data)
                            is_new = True
                        self.task_queue_lock.release()
                        if is_new:
                            self.new_queues_lock.acquire()
                            self.new_queues.append(pqueue)
                            self.new_queues_lock.release()
                for tid in to_delete:
                    del self.pending_queue_data[pqueue][tid]
            self.pending_task_queue_lock.release()
        return tasks_ids
    
    def check_dependencies_per_task(self, taskid):
        for fileid in self.task_to_file[taskid]['outputs']:
            dependent_tasks = []
            try:
                dependent_tasks = self.file_to_task[
                            hashlib.sha1(fileid.encode()).hexdigest()]['tids']
            except:
                pass
            for taskid2 in dependent_tasks:
                self.task_to_file[taskid2]['cinputs'] = self.task_to_file[taskid2]['cinputs'] + 1
                if self.task_to_file[taskid2]['cinputs'] == self.task_to_file[taskid2]['tinputs']:
                    # put in ready queue
                    self.pending_task_queue_lock.acquire()
		    try:
                        task = self.pending_queue_data[self.task_to_file[taskid2]['queueid']][taskid2]
                    except:
			self.pending_task_queue_lock.release()
                        continue
		    self.pending_task_queue_lock.release()
                    data = {
                            'id': taskid2,
                            'exec': task['exec'],
                            'params': task['params']}
                    self.task_queue_lock.acquire()
                    is_new = False
                    try:
                        self.task_queue_data[self.task_to_file[taskid2]['queueid']].append(data)
                    except:
                        self.task_queue.append(self.task_to_file[taskid2]['queueid'])  # FCFS like
                        self.task_queue_data[self.task_to_file[taskid2]['queueid']] = deque()
                        self.task_queue_data[self.task_to_file[taskid2]['queueid']].append(data)
                        is_new = True
                    self.task_queue_lock.release()
                    if is_new:
                        self.new_queues_lock.acquire()
                        self.new_queues.append(self.task_to_file[taskid2]['queueid'])
                        self.new_queues_lock.release()
		    self.pending_task_queue_lock.acquire()
                    del self.pending_queue_data[
                                self.task_to_file[taskid2]['queueid']][taskid2]
                    self.pending_task_queue_lock.release()

    def garbage_collect(self, taskid):
        for fileid in self.task_to_file[taskid]['inputs']:
            try:
                self.file_to_task[
                                hashlib.sha1(
                                    fileid.encode()).hexdigest()]['ctids'] = self.file_to_task[
                                hashlib.sha1(
                                    fileid.encode()).hexdigest()]['ctids'] + 1
                if self.file_to_task[
                                hashlib.sha1(
                                    fileid.encode()).hexdigest()]['ctids'] == self.file_to_task[
                                hashlib.sha1(
                                    fileid.encode()).hexdigest()]['ntids']:
                                # now it is safe to delete this file (is it?)
                    try:
                        print "deleting file ", fileid, "ctids=", \
                            self.file_to_task[hashlib.sha1(fileid.encode()).hexdigest()]['ctids'] \
                            , "ntids=", self.file_to_task[hashlib.sha1(fileid.encode()).hexdigest()]['ntids']
                        self.logger.debug("File: %s dependent_tasks: %s" %
                                      (fileid, self.file_to_task[
                                        hashlib.sha1(
                                        fileid.encode()).hexdigest()]['ntids']))
                                    # os.remove(fileid)
                                # if failed, report it back to the user ##
                    except OSError as e:
                        print "Error: %s - %s." % (e.filename, e.strerror)
            except:
                print "exception for file ", fileid

    def get_result(self):
        while self.running:
            self.pending_task_queue_lock.acquire()
            to_delete = []
            for pqueue in self.pending_queue_data:
                if len(self.pending_queue_data[pqueue]) == 0:
                    to_delete.append(pqueue)
            for pqueue in to_delete:
                del self.pending_queue_data[pqueue]
            self.pending_task_queue_lock.release()
            tasks_ids = self.get_taskids()
            if len(tasks_ids) == 0:
                continue
            for taskid in tasks_ids:
                # if missing files were generated put task in ready queue
		try:
		    self.check_dependencies_per_task(taskid)
		except:
		    traceback.print_exc()
	'''
            for taskid in tasks_ids:
                try:
                    self.garbage_collect(taskid)
                except:
                    print "I cannot find ", taskid, "in task_to_file"
	'''

    def msg_process_callback(self, message):
        message_type = message[3]
        try:
	    if message_type == PROTOCOL_HEADERS['CLIENT']:
                self.process_client_message(message)
            elif message_type == PROTOCOL_HEADERS['WORKER']:
                self.process_worker_message(message)
	except:
	    traceback.print_exc()


    def process_queue(self, data, qid):
	task_data = pickle.loads(data)
	self.task_queue_lock.acquire()
        self.pending_task_queue_lock.acquire()
	for task in task_data:
	    self.process_queue_task(task, qid) 
	self.pending_task_queue_lock.release()
        self.task_queue_lock.release() 


    def process_queue_task(self, task_data, qid):
        self.task_id = self.task_id + 1
        splited_inputs = task_data['inputs'].split()
        total_inputs = len(splited_inputs)
        current_inputs = 0
        for inputf in splited_inputs:
            if os.path.isfile(inputf):
                current_inputs = current_inputs + 1
            #else:
            #    print "Missing file: ", inputf
            try:
                self.file_to_task[
                        hashlib.sha1(
                            inputf.encode()).hexdigest()]['tids'].append(
                        self.task_id)
                self.file_to_task[
                        hashlib.sha1(
                            inputf.encode()).hexdigest()]['ntids'] = self.file_to_task[
                        hashlib.sha1(
                            inputf.encode()).hexdigest()]['ntids'] + 1
            except:
                self.file_to_task[
                        hashlib.sha1(
                            inputf.encode()).hexdigest()] = {
                        'ntids': 1,
                        'tids': [],
                        'ctids': 0}
                self.file_to_task[
                        hashlib.sha1(
                            inputf.encode()).hexdigest()]['tids'] = [
                        self.task_id]
        self.task_to_file[self.task_id] = {
                'queueid': qid,
                'tinputs': total_inputs,
                'cinputs': current_inputs,
                'inputs': splited_inputs,
                'outputs': task_data['outputs'].split()}
        if current_inputs == total_inputs:
            print "Putting task ", self.task_id, " in active queue", qid
            if qid not in self.task_queue:
                try:
                    self.new_queues_lock.acquire()
                    self.new_queues.append(qid)
                    self.new_queues_lock.release()
                    self.task_queue.append(qid)  # FCFS like
                    self.task_queue_data[qid] = deque()
                except:
                    traceback.print_exc()
            self.task_queue_data[qid].append(
                    {'id': self.task_id, 'exec': task_data['exec'], 'params': task_data['params']})
        else:
            self.logger.info(
                    "Putting task %s in pending queue, total inputs: %s" %
                    (self.task_id, total_inputs))
            task_info = {
                    'id': self.task_id,
                    'exec': task_data['exec'],
                    'params': task_data['params'],
                    'inputs': splited_inputs}
            try:
                self.pending_queue_data[qid][self.task_id] = task_info
            except:
                self.pending_queue_data[qid] = {}
                self.pending_queue_data[qid][self.task_id] = task_info
 
    def process_status(self, qid):
        self.task_queue_lock.acquire()
        ntasks = len(self.task_queue_data[qid])
        self.task_queue_lock.release()
        reply = [
                message[0],
                PROTOCOL_HEADERS['RSMNG'],
                'status',
                str(ntasks)]
        self.server_thread.put_request_in_queue(reply)

        
    def process_wait(self, qid, message):
        self.workers_lock.acquire()
	self.workers_emtpy = 0
        self.workers_empty_dict = {}
	self.workers_lock.release()
        self.waiting_clients.append(message[0])
        
    def process_delete(self, data):
        worker_id = 'sched-' + data
        with open(os.devnull, 'w') as devnull:
        	proc = subprocess.Popen(
                    "ssh %s \" pkill -9 local_resourcem \" " %
                    data,
                    stdout=devnull,
                    stderr=devnull)
        	proc.wait()
        # we delete all info about the worker....
        self.workers_lock.acquire()
        self.workers.remove(worker_id)
        del self.workers_data[worker_id]
	self.workers_lock.release()

    def process_client_message(self, message):
        tmp_msg = message[4:]
        qid = message[1]
        action = tmp_msg[0]
        data = None
        if len(tmp_msg) > 1:
            data = tmp_msg[1]
	print "I receive message for queue ", qid, " action: ", action
        if action == 'queue':
            if data is None:
                #print "Missing task information"
                return
            self.process_queue(data, qid)
        elif action == 'status':
            self.process_status(qid)
        elif action == 'wait':
            self.process_wait(qid, message)
        elif action == 'clear':
            print "I will empty the worker queues"
            for worker in workers:
                self.server_thread.put_request_in_queue(
                    [worker, PROTOCOL_HEADERS['RSMNG'], 'empty'])
        elif action == 'delete':
            self.process_delete(data)
        else:
            print "Not implemented yet"


    def process_worker_nonempty_queue_static(self, tasks_per_queue, answer):
        queue_id = self.task_queue[0]
	ntasks = min(
                len(self.task_queue_data[queue_id]), int(tasks_per_queue))
        for i in range(0, ntasks):
            task = self.task_queue_data[queue_id].popleft()
            answer['tasks'].append(task)
        if len(self.task_queue_data[queue_id]) == 0:
            self.delete_queue(queue_id)
	return answer
                
    def process_worker_nonempty_queue_dynamic(self, queue_id, tasks_per_queue, answer):
        # send from each queue
	print "[Empty] Asked ", queue_id, "tasks per queue ", tasks_per_queue
        to_delete = []
	for qid in self.task_queue_data:
            # if the active queue len is smaller than what the worker asked
            # and the pending queue len is larger than that, do
            # not send?
            req_tasks = int(tasks_per_queue)
	    self.pending_task_queue_lock.acquire()
	    pending_qid = self.pending_queue_data.get(qid)
	    pending_qid_len = 0
	    if pending_qid != None:
	        pending_qid_len = len(pending_qid)
	    self.pending_task_queue_lock.release()
            #if pending_qid:
            #    if pending_qid_len > req_tasks and len(
            #        self.task_queue_data[qid]) < req_tasks:
	    #	    print "I have in queue less than asked and in pending queue more!"
            #        continue
            ntasks = min(
                        len(self.task_queue_data[qid]), req_tasks)
            answer['queues'][qid] = ntasks
            print "Asked for ", tasks_per_queue, "Sending ", ntasks, "from queue ", qid, \
                                " from max ntasks ", len(self.task_queue_data[qid])
            for i in range(0, ntasks):
                task = self.task_queue_data[qid].popleft()
                answer['tasks'].append(task)
            if ntasks > 0 and len(
                self.task_queue_data[qid]) == 0:
		to_delete.append(qid)                

	for qid in to_delete:
	    self.delete_queue(qid)
	return answer

    def process_worker_nonempty_queue_static1(self, tasks_per_queue, answer):
        # send only from the first queue
	queue_id = self.task_queue[0]
        if queue_id not in tasks_per_queue:
            ntasks = min(len(self.task_queue_data[queue_id]), int(tasks_per_queue.values()[0]))
        else:
            ntasks = min(len(self.task_queue_data[queue_id]), int(tasks_per_queue[queue_id]))
        for i in range(0, ntasks):
            task = self.task_queue_data[queue_id].popleft()
            answer['tasks'].append(task)
        if len(self.task_queue_data[queue_id]) == 0:
            self.delete_queue(queue_id)
	return answer 
                
    def process_worker_nonempty_queue_dynamic1(self, tasks_per_queue, answer):
	print "####################### Asked ", tasks_per_queue
        for qid in tasks_per_queue:
            # for each queue from which the worker asks check
            # if I have enough tasks to send
            req_tasks = int(tasks_per_queue[qid])
	    self.pending_task_queue_lock.acquire()
            if self.pending_queue_data.get(qid) and self.task_queue_data.get(qid):
                if len(self.task_queue_data) > 1 and len(self.pending_queue_data[qid]) > req_tasks and len(
                        self.task_queue_data[qid]) < req_tasks:
		    self.pending_task_queue_lock.release()
                    continue
            self.pending_task_queue_lock.release()
	    try:
                ntasks = min(len(self.task_queue_data[qid]), req_tasks)
            except:
                answer['queues'][qid] = -1
                ntasks = -1
                continue
	    answer['queues'][qid] = ntasks
            if ntasks > 0:
		print "Sending ", ntasks, " from queue ", qid
                for i in range(0, ntasks):
                    task = self.task_queue_data[qid].popleft()
                    answer['tasks'].append(task)
                if len(self.task_queue_data[qid]) == 0:
                    self.delete_queue(qid)
	to_delete = []
	return answer

    def process_worker_nonempty_queue(self, worker_id, type, data):
        ''' FCFS queue for the static policy '''
        randn = 0
        queue_id = self.task_queue[randn]
        answer = {'queues': {}, 'tasks': []}
        if type == 'task_empty':
            tasks_per_queue = data
            if config.POLICY == 'static':
                # send from the first queue
		answer = self.process_worker_nonempty_queue_static(tasks_per_queue, answer)
            else:
                answer =self.process_worker_nonempty_queue_dynamic(queue_id, tasks_per_queue, answer)
        else:
            tasks_per_queue = pickle.loads(data)
	    print tasks_per_queue
            if config.POLICY == "static":
                answer = self.process_worker_nonempty_queue_static1(tasks_per_queue, answer)
            else:
                answer = self.process_worker_nonempty_queue_dynamic1(tasks_per_queue, answer)
	    for qid in self.task_queue_data:
                if qid not in tasks_per_queue:
                    answer['queues'][qid] = 0
        print "Sending ", len(answer['tasks']), answer['queues'], " to worker ", worker_id
        # TODO : piggyback the ids of other queues, if no queues > 1
        ''' if we don't have any data left in the queue we delete it '''
        ''' if the worker sent us an empty message reset it '''
        self.workers_lock.acquire()
	if self.workers_empty_dict.get(worker_id):
            del self.workers_empty_dict[worker_id]
	self.workers_lock.release()
        return answer

    def process_worker_message(self, message):
        tmp_msg = message[4:]
        type = tmp_msg[0]
        data = None
        first_worker = False
        self.workers_lock.acquire()
        if message[0] not in self.workers:
            self.workers.append(message[0])
	    self.workers_data[message[0]] = 0
            #first_worker = True
        if len(tmp_msg) > 1:
            data = tmp_msg[1]
        if len(tmp_msg) > 2:
            data2 = pickle.loads(tmp_msg[2])
            computed_tasks = data2['ran']
	    worker_current_size = data2['qsize']
	    self.workers_data[message[0]] = float(worker_current_size)
	    if len(computed_tasks) > 0:
                self.result_queue.put(computed_tasks)
	if type == 'task_empty':
	    self.workers_data[message[0]] = 0
        self.workers_lock.release()
	if type == 'task' or type == 'task_empty':
            ''' send x tasks to it '''
            ''' this is a hack; it works only with one client '''
            self.task_queue_lock.acquire()
	    task_queue_len = len(self.task_queue)
	    self.task_queue_lock.release()
	    self.pending_task_queue_lock.acquire()
	    pending_task_queue_len = len(self.pending_queue_data)
	    self.pending_task_queue_lock.release()
	    if type == 'task_empty' and task_queue_len == 0 \
                    and pending_task_queue_len == 0 \
                    and len(self.waiting_clients) > 0:
		self.workers_lock.acquire()
                if not self.workers_empty_dict.get(
                        message[0]) and not first_worker:
                    self.workers_empty_dict[message[0]] = 1
		self.workers_lock.release()
            answer = {'queues': {}, 'tasks': []}
            wake_client = False
            self.task_queue_lock.acquire()
            task_queue_len = len(self.task_queue)
	    sent_tasks = False
	    if task_queue_len > 0:
		sent_tasks = True
                answer = self.process_worker_nonempty_queue(message[0], type, data)
            else:
		print self.workers_empty_dict
		self.workers_lock.acquire()
                if pending_task_queue_len == 0 and len(
                        self.workers_empty_dict) >= len(
                        self.workers):
                    wake_client = True
		self.workers_lock.release()
            self.task_queue_lock.release()
	    if len(answer['tasks']) > 0:
		print "Sending ", answer, " to worker ", message[0]
                data = pickle.dumps(answer)
		self.server_thread.put_request_in_queue(
                    [message[0], PROTOCOL_HEADERS['RSMNG'], 'task', data])
            if wake_client:
                for client in self.waiting_clients:
                    print "Sending wake up message to ", client
                    self.server_thread.put_request_in_queue(
                        [client, PROTOCOL_HEADERS['RSMNG'], 'done'])
                self.waiting_clients = []
                self.workers_empty = 0
		return
	    if sent_tasks:
		return
	    ''' If my queues are empty, we apply the work stealing algorithm.... '''
	elif type == 'output':
            ''' un-serialize the output and append it to a log '''
            output = pickle.loads(data)
            self.task_logger.info(output)

    ''' Main scheduling LOOP '''

    def run(self):
        self.server_thread.start()
        while self.running:
            try:
                self.logger.debug("Scheduling.......")
                ''' compute the number of workers and data nodes for each
                    scheduling period '''
		self.task_queue_lock.acquire()
                print "*** Task queue is: ", self.task_queue
                for aqueue in self.task_queue:
                    print "Queue ", aqueue, "len ", len(self.task_queue_data[aqueue])
                self.task_queue_lock.release()
		self.pending_task_queue_lock.acquire()
		print "Task pending queue is: ", self.pending_queue_data.keys()
                for pqueue in self.pending_queue_data:
                    print "Queue ", pqueue, "len ", len(self.pending_queue_data[pqueue])
                self.pending_task_queue_lock.release()
		if self.running:
                    time.sleep(config.WAITTIME)
            except KeyboardInterrupt:
                self.shutdown()
            except:
                traceback.print_exc(self.logger.path)
                try:
                    time.sleep(config.WAITTIME)
                except KeyboardInterrupt:
                    self.shutdown()
        print "Stopping communication thread...."
        self.logger.info("Stopping communication thread....")
        self.server_thread.stop()
        print "Joining communication thread...."
        self.logger.info("Joining communication thread....")
        self.server_thread.join()
        print "DONE"
        self.logger.info("DONE")
        return

    def shutdown(self):
        print "Received signal to shutdown. "
        self.logger.info("Received signal to shutdown. Will wait for the end of the \
                    scheduling period")
        self.running = False
