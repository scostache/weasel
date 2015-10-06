import os
import sys
import time
import pickle
import zmq
import threading
import traceback
import socket
import re
import math
import weasel.etc.config as config
from subprocess import *
import hashlib
import psutil
from weasel.utils.notification import *
from weasel.utils.threads import *
from weasel.utils.logger import WeaselLogger
from weasel.worker.node_monitor import NodeMonitor


class NodeScheduler(object):

    def __init__(self):
        self.identity = 'sched-' + socket.gethostbyname(socket.gethostname())
        self.sched_client_thread = ZmqConnectionThread(
            self.identity,
            zmq.DEALER,
            config.SCHEDULER+":" + str(config.ZMQ_SCHEDULER_PORT),
            self.callback)
        self.monitor_thread = NodeMonitor()
        self.running = True
        logfile = config.LOGDIR + "/local_scheduler.log"
        self.logger = WeaselLogger('local_scheduler', logfile)
        self.capacity = self.monitor_thread.capacity
        self.max_tasks_to_run = {}
        ''' the starting number of tasks is defined based on the slot size '''
        self.ntasks_to_ask = 1
        self.task_id = 1
        self.time_asked_first = time.time()
        self.time_from_last_ask = -1
        ''' this is to keep track of number of running tasks ? '''
        self.running_task = 0
        self.nran_tasks = []
        self.time_from_last_ask = time.time()
        self.queues_asked_for = []
        self.current_ntasks = 1
        self.has_new_task = False
        self.is_profiling = False
        self.first_task = False
        self.task_data = {}
        self.t_avg = {}
        self.task_data_lock = threading.Lock()
        self.running_task_lock = threading.Lock()
        self.average_utilization = {'cpu': 0.0, 'memory': 0.0, 'network': 0.0}
        self.average_task_exec_time = 0.0
        self.sleep_time = config.WAITTIME
        self.past_speed_changes = []
        # 'id': id, 'tpool': threadPool, 'rvector': resource_characteristics
        self.queue_data = {}
	self.task_time = 1
        self.queue_data_lock = threading.Lock()
        self.has_new_queue = False
        self.new_queues = []
        self.message_to_send = None
	''' this is to control how many tasks to run in parallel'''
        self.logger.info("NodeScheduler started...")
        self.nrunning_past_period = []

    def profile(self, nrunning):
        pass

    def change_work_queue(self, nrunning, nrunning_past, avg_time, avg_cpu):
        pass

    def run_task(self, arg):
	command_id = arg['id']
        command = arg['exec'] +' ' + arg['params']
	qid = arg['qid']
	myid = threading.current_thread().ident
        self.running_task_lock.acquire()
        self.running_task = self.running_task + 1
        self.running_task_lock.release()
        ''' this also marks that at least one task runs on the node ... '''
        ''' here I need to put it in the queue of tasks that the monitor will watch over '''
        memory_average = 0.0
        cpu_average = 0.0
        nreads = 0
        nwrites = 0
        nbytesread = 0
        nbyteswritten = 0
        time_intervals = 0
        start_time = time.time()
        proc = psutil.Popen(command, shell=True,
                            stdout=PIPE, stderr=PIPE)
	self.task_data[myid]['lock'].acquire()
	self.task_data[myid]['proc'] = proc
	self.task_data[myid]['ctask'] = arg
	self.task_data[myid]['lock'].release()
        out, err = proc.communicate()
        end_time = time.time()
        self.task_data[myid]['lock'].acquire()
        if self.task_data[myid]['task'].get(qid) == None:
            self.task_data[myid]['task'][qid] = []
            self.external_change = True
        self.task_data[myid]['task'][qid].append(
            [end_time - start_time, 100 * (end_time - start_time) / (end_time - start_time)])
        self.task_data[myid]['lock'].release()
        self.running_task_lock.acquire()
        self.running_task = self.running_task - 1
        self.nran_tasks.append(command_id)
        self.running_task_lock.release()

    def get_total_queue_size(self):
        queue_size = 0
        self.queue_data_lock.acquire()
        for qid in self.queue_data:
	    #print "Queue ", qid, " size ", self.queue_data[qid]['tpool'].tasks.qsize()
            queue_size = queue_size + \
                self.queue_data[qid]['tpool'].tasks.qsize()
        self.queue_data_lock.release()
        return queue_size

    def get_tasks_to_ask(self, nrunning):
        tasks_to_ask = {}
        self.queues_asked_for = []
        queue_size = self.get_total_queue_size()
        if queue_size + nrunning == 0 and not self.is_profiling:
            return (tasks_to_ask, queue_size)
        self.queue_data_lock.acquire()
        for qid in self.queue_data:
	    tasks_to_ask[qid] = 0
            self.queue_data[qid]['asked'] = 0
            self.queue_data[qid]['recv'] = 0
            qsize = self.queue_data[qid]['tpool'].tasks.qsize()
            if qsize > 2 * self.max_tasks_to_run[qid] and self.max_tasks_to_run[qid] != -1:
                continue
            if qsize == 0:
                tasks_to_ask[qid] = max(10, 2 * self.max_tasks_to_run[qid])
            else:
                if qsize > 2 * self.max_tasks_to_run[qid] and self.max_tasks_to_run[qid] != -1:
                    continue
                elif qsize < 2 * self.max_tasks_to_run[qid]:
                    tasks_to_ask[qid] = 2
            self.queues_asked_for.append(qid)
            self.queue_data[qid]['asked'] = tasks_to_ask[qid]
        self.queue_data_lock.release()
        return (tasks_to_ask, queue_size)

    def wait_and_ask(self):
        while self.running:
            # check at 0.2 seconds
            time.sleep(0.2)
            # how much time is passed from the last time we asked the rmng
            ctime = time.time()
            if ctime - self.time_from_last_ask > 2 * config.WAITTIME:
                # here we mark the queues as dead
                for qid in self.queues_asked_for:
		    if self.queue_data[qid]['tpool'].tasks.qsize() == 0:
                        print "@@@@@@@@@@@@@@@@@@@  I mark queue ", qid, " as dead because I don't have tasks for it"
                        self.max_tasks_to_run[qid] = -1
            self.running_task_lock.acquire()
            nrunning = self.running_task
            self.nrunning_past_period.append(nrunning)
            task_data_to_send = {'ran': self.nran_tasks[:]}
            self.nran_tasks = []
            self.running_task_lock.release()
            resources = {'cpu': 0, 'memory': 0, 'network': 0}
            if self.is_profiling:
                self.profile(nrunning)
            (tasks_to_ask, queue_size) = self.get_tasks_to_ask(nrunning)
	    #print "Asking for tasks: ", tasks_to_ask, queue_size
	    task_data_to_send['qsize'] = queue_size * self.task_time
            pickled_data = pickle.dumps(task_data_to_send)
	    if self.is_profiling and config.POLICY == 'dynamic3':
                if qsize + nrunning == 0 and not self.first_task:
                    self.sched_client_thread.put_request_in_queue(
                        [self.identity, PROTOCOL_HEADERS['WORKER'], 'task_empty',
                         str(2 * self.current_ntasks), pickled_data])
                    self.first_task = True
                continue
            elif len(tasks_to_ask) > 0:
                self.sched_client_thread.put_request_in_queue(
                    [self.identity, PROTOCOL_HEADERS['WORKER'], 'task',
                     pickle.dumps(tasks_to_ask), pickled_data])
	    self.message_to_send = pickled_data

    def process_task(self, task):
        tmp = task.split(';')
        task_name = tmp[-1].split()[0].split('/')[-1]
        new_task = False
        # I have new tasks!!
        if task_name not in self.monitor_thread.tasks_to_monitor:
            new_task = True
            self.is_profiling = True
            self.monitor_thread.add_task_to_monitor(task_name)
        return new_task

    def add_task_to_queues(self, tasks):
        if len(tasks['queues']) > 0:
	    print tasks['queues']
            self.queue_data_lock.acquire()
            for queue in tasks['queues']:
                if not self.queue_data.get(queue):
                    self.new_queues.append(queue)
                    self.max_tasks_to_run[queue] = 0
                    self.queue_data[queue] = {
                        'qid': queue,
			'elapsed':0,
			'tavg': 0,
			'thoughput':0,
                        'asked': 0,
                        'recv': 0,
                        'type': "",
			'tpool': ThreadPool(
                            0,
                            self.task_data),
                        'resource': ""}  # this contains the resource vector of a task
                    self.has_new_queue = True
                if tasks['queues'][queue] == -1:
                    print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Queue ", queue, " is empty!"
                    self.max_tasks_to_run[queue] = -1
            self.queue_data_lock.release()
        self.queue_data_lock.acquire()
        for task in tasks['tasks']:
	    #print "Adding tasks to queues: ", task
            ''' here: if the task does not exist in my history:
                shrink the pool at 1 task and enter the profiling mode
                profiling mode = record resource util for the first 10 tasks '''
            qid = hashlib.sha1(task['exec'].encode()).hexdigest()
            self.has_new_task = self.has_new_task | self.process_task(
                task['exec'])
	    task['qid'] = qid
            self.add_task_to_queue(self.queue_data[qid]['tpool'], task)
            self.queue_data[qid]['recv'] = self.queue_data[qid]['recv'] + 1
        self.queue_data_lock.release()

    def callback(self, frames):
        ''' this is a message from the server '''
	command = frames[2]
        data = None
        if len(frames) > 3:
            data = frames[3]
        if command == 'shutdown':
            self.shutdown(None)
        elif command == 'task':
            self.time_from_last_ask = time.time()
            tasks = pickle.loads(data)
            self.add_task_to_queues(tasks)
	elif command == 'empty':
            for qid in self.queue_data:
                self.empty_queue(self.queue_data[qid]['tpool'])
	else:
	    print "No callback for this message!"


    def add_task_to_queue(self, queue, task):
        queue.add_task(self.run_task, task)

    def empty_queue(self, queue):
        while not queue.empty():
            try:
                queue.get(False)
            except Empty:
                continue
            queue.task_done()

    def shutdown(self, data):
        self.running = False

    def log_node_utilization(self):
        median = self.monitor_thread.get_median_utilization()
        histo_util = self.monitor_thread.get_utilization_by_histogram()
        data = self.monitor_thread.get_data()
        cpu_sum = median['cpu'] + median['cpu_idle'] + \
            median['cpu_sys'] + median['cpu_io']
        if cpu_sum == 0:
            real_value = 0
        else:
            real_value = 100 * (median['cpu']) / cpu_sum
        self.logger.info(
            "Median utilization/2secs: %s %s %s %s" %
            (median['cpu'],
             median['memory'],
                median['network'],
                100 *
                median['cpu_io'] /
                cpu_sum))
        self.logger.info(
            "Histo utilization: %s %s %s" %
            (histo_util['cpu'],
             histo_util['memory'],
             histo_util['network']))

    def is_ok_to_ask(self):
        return True

    def empty_task_data(self):
        for tid in self.task_data:
            self.task_data[tid]['lock'].acquire()
            self.task_data[tid]['task'] = {}
            self.task_data[tid]['lock'].release()

    def check_empty_queues(self):
        queues_empty = True
        self.queue_data_lock.acquire()
        for qid in self.queue_data:
            queues_empty = queues_empty & self.queue_data[
                qid]['tpool'].tasks.empty()
        self.queue_data_lock.release()
        return queues_empty


    def compute_stats(self, task_data, avg_time, avg_cpu):
        total_len = 0
        try:
            for tid in task_data:
                task_data[tid]['lock'].acquire()
                for task in task_data[tid]['task']:
                    if not avg_time.get(task):
                        avg_time[task] = 0
                        avg_cpu[task] = 0
                    for data in task_data[tid]['task'][task]:
                        avg_time[task] = avg_time[task] + data[0]
                        avg_cpu[task] = avg_cpu[task] + data[1]
                    total_len = total_len + \
                            len(task_data[tid]['task'][task])
                    if not self.is_profiling:
                        # leave the last value
                        while len(task_data[tid]['task'][task]) > 0:
                            task_data[tid]['task'][task].pop(0)
                task_data[tid]['lock'].release()
            for task in avg_time:
                avg_time[task] = avg_time[task] / total_len
                avg_cpu[task] = avg_cpu[task] / total_len
	    self.empty_task_data()
        except:
            traceback.print_exc()

    def run(self):
        self.sched_client_thread.start()
        self.monitor_thread.start()
        finishing_tasks_thread = Thread(target=self.wait_and_ask)
        finishing_tasks_thread.start()
        '''  I have: - the monitoring thread
        - the communication thread
        - the thread that waits to ask for more tasks
        '''
        while self.running:
            ''' if queue is empty and no other tasks are running: ask for task to the scheduler '''
            ''' else if tasks are running check the utilization and ask for more/less '''
            self.log_node_utilization()
            task_data = self.monitor_thread.get_task_data()
            total_util = {'cpu': 0, 'memory': 0}
            for task in task_data:
                for data in task_data[task]:
                    total_util['cpu'] = total_util[
                        'cpu'] + data[0][0] / len(task_data[task])
                    total_util['memory'] = total_util[
                        'memory'] + data[1] / len(task_data[task])
            self.logger.info(
                "Total utilization of the other processes is: %s %s" %
                (total_util['cpu'], total_util['memory']))
            # count the total number of slots
            self.running_task_lock.acquire()
            nrunning = self.running_task
            nrunning_past = self.nrunning_past_period[:]
            self.nrunning_past_period = []
            self.running_task_lock.release()
            for task in self.max_tasks_to_run:
                self.logger.info(
                    "%s Running tasks: %s" %
                    (task, self.max_tasks_to_run[task]))
            if self.check_empty_queues() and nrunning == 0:
                if self.is_ok_to_ask():
                    ''' I have finished all my tasks, ask for random task from the resource mng '''
		    print "Sending task_empty message!"
                    self.sched_client_thread.put_request_in_queue(
                            [self.identity, PROTOCOL_HEADERS['WORKER'], 'task_empty', str(2 * self.capacity['cores'])])
            avg_time = {}
            avg_cpu = {}
            task_data = self.task_data
            self.compute_stats(task_data, avg_time, avg_cpu)
            # now is the time to remove the data from the dead threads
            self.queue_data_lock.acquire()
            for qid in self.queue_data:
                self.queue_data[qid]['tpool'].dict_lock.acquire()
                for tid in self.queue_data[qid]['tpool'].deleted_workers:
                    if self.task_data.get(tid):
                        del self.task_data[tid]
                self.queue_data[qid]['tpool'].deleted_workers = []
                self.queue_data[qid]['tpool'].dict_lock.release()
            taskid = 0
	    max_task_time = 0
            for task in avg_time:
		if config.POLICY != 'static':
                    if avg_time[task] == 0:
		        self.queue_data[task]['elapsed'] = self.queue_data[task]['elapsed'] + config.WAITTIME
		    else:
		        self.queue_data[task]['elapsed'] = 0
		        self.queue_data[task]['tavg'] = (self.queue_data[task]['tavg'] + avg_time[task])/2
		if avg_time[task] > max_task_time:
                    max_task_time = avg_time[task] 
		print task, "Avg_time: ", avg_time[task]
                self.logger.info(
                        "%s Avg_time: %s" %
                        (task, avg_time[task]))
                self.logger.info(
                        "%s Task speed: %s" %
                        (task, self.max_tasks_to_run[task] / avg_time[task]))
                self.logger.info(
                        "%s Task util: %s" %
                        (task, avg_cpu[task]))
                self.past_speed_changes.append(
                        self.max_tasks_to_run[task] /
                        avg_time[task])
                if len(self.past_speed_changes) > 4:
                    self.past_speed_changes.pop(0)
                taskid = taskid + 1
	    
	        if config.POLICY != 'static' and self.queue_data[task]['type'] == "":
		    if self.queue_data[task]['elapsed'] > config.T_LONG or \
				self.queue_data[task]['tavg'] > config.T_LONG:
		        self.queue_data[task]['type'] = 'long'
		    if self.queue_data[task]['tavg'] < config.T_LONG:
		        self.queue_data[task]['type'] = 'short'
	    self.queue_data_lock.release()
            max_avg_time = 0
	    self.task_time = max_task_time
            self.change_work_queue(nrunning, nrunning_past, avg_time, avg_cpu)
            self.logger.info("Ran %s tasks" % self.task_id)
	    self.logger.debug("Sleeping: %s" % self.sleep_time)
            self.monitor_thread.max_data_buffer_len = int(
                self.sleep_time /
                config.MONITOR_PERIOD)
            time.sleep(self.sleep_time)
        finishing_tasks_thread.join()
        for qid in self.queue_data:
            self.queue_data[qid]['tpool'].wait_completion()
        self.sched_client_thread.stop()
        self.monitor_thread.shutdown()
        self.monitor_thread.join()
        self.sched_client_thread.join()
