from weasel.worker.node_scheduler import NodeScheduler
import math
import time
import random
import threading
import hashlib
import weasel.etc.config as config
from weasel.utils.estimation import EWMA, MA


class pastHistoryValue(object):
    def __init__(self, k, alpha, resource_dict):
	self.cpu = MA(k)
	self.cpu_io = MA(k)
	self.memory = MA(k)
	self.network = MA(k)
	self.disk = MA(k)
	self.cpu.get_next_value(resource_dict['cpu'])
	self.cpu_io.get_next_value(resource_dict['cpu_io'])
	self.memory.get_next_value(resource_dict['memory'])
	self.network.get_next_value(resource_dict['network'])
	self.disk.get_next_value(resource_dict['disk'])

    def get_value(self):
	return {'cpu': self.cpu.get_estimated_value(),
		'cpu_io': self.cpu.get_estimated_value(),
		'disk': self.disk.get_estimated_value(), 
		'memory': self.memory.get_estimated_value(),
		'network': self.network.get_estimated_value()}

    def push_value(self, resource_dict):
	self.cpu.get_next_value(resource_dict['cpu'])
	self.cpu_io.get_next_value(resource_dict['cpu_io'])
	self.disk.get_next_value(resource_dict['disk'])
        self.memory.get_next_value(resource_dict['memory'])
        self.network.get_next_value(resource_dict['network'])

    def reset(self):
	self.cpu.reset()
	self.memory.reset()
	self.cpu_io.reset()
	self.disk.reset()


class Adapt(object):
    def __init__(self, qid, tqueue):
	self.max_past_history = {'ntasks': 0, 'resource': 0}
	self.bad_points = []
	self.past_nb_of_tasks = []  
	self.external_change = False
	self.current_task = ""
	self.ntasks_before_contention = []
	self.restart = False
	self.qid = qid
	self.tqueue = tqueue
	self.past_resource = 'cpu'
	self.current_ntasks = 1
	self.max_tasks_to_run = 0
	self.last_action = 0
	self.same_dir = 0
	self.start_exp_after = 4
	self.start_exp_after_increase = 1
	self.start_exp_after_decrease = 2
	self.tasks_increase = 1
	self.change_for_one_task = {'resource': "", 'diff': 0.0}
	self.max_past_history = {'ntasks': 0, 'resource': 0}
	self.is_contention = False
	self.external_change = False
	self.jump_after = 3 # 3 oscilations before jump
	self.number_of_oscilations =0 
	self.past_speed_changes = []
	self.past_history = {}
	self.past_changes = []
	self.is_profiling = False
	self.past_max_tasks = 0 
	self.restored = False
	self.resource_delta_history = []
	self.buffer_len = 3


    def check_resource_deltas(self):
        diff = self.determine_past_change_0()
	resource = 'cpu'
        max_resource = diff['cpu'] + diff['cpu_io']
        if abs(diff['memory']) > abs(max_resource):
            max_resource = diff['memory']
            resource = 'memory'
        if abs(diff['network']) > abs(max_resource):
           max_resource = diff['network']
           resource = 'network'
        if resource == 'cpu':
            max_resource = diff['cpu']
        #print resource, max_resource
	return (diff, resource, max_resource)


    def determine_past_change_0(self):
        #print self.resource_delta_history
        if len(self.resource_delta_history) < 2:
            return self.resource_delta_history[0]
        past_value = self.resource_delta_history[-1]
        second_past_value = self.resource_delta_history[-2]
        diff = {'cpu':0, 'memory':0, 'network':0, 'cpu_io':0}
        for r in diff:
            if second_past_value[r] == 0:
                continue
            ''' I compute the variation as compared to the previous value, 
                        positive: is increasing
                        negative: is decreasing '''
            diff[r] = past_value[r] - second_past_value[r]
        return diff


    def add_to_history(self, resource_delta):
        self.resource_delta_history.append(resource_delta)
        if len(self.resource_delta_history) > self.buffer_len:
            self.resource_delta_history.pop(0)


    def update_resource_history(self, past_history_max_len, past_value_w, actual_value):
        if self.past_history.get(self.max_tasks_to_run) != None:
            self.past_history[self.max_tasks_to_run]['resource'].push_value(actual_value)
            for r in ('cpu', 'memory', 'network', 'cpu_io'):
                if self.past_history[self.max_tasks_to_run]['max'][r] < actual_value[r]:
                    self.past_history[self.max_tasks_to_run]['max'][r] = actual_value[r]
        else:
            self.past_history[self.max_tasks_to_run] = {'change': 0,
                                                        'nchanges': 0,
                                                        'resource': pastHistoryValue(past_history_max_len, past_value_w, actual_value),
                                                        'max': actual_value}
        if self.change_for_one_task['resource'] != "":
            #print self.qid, "Current resource util: ", self.past_history[self.max_tasks_to_run]['resource'].get_value()[self.change_for_one_task['resource']]
            current_rutil = self.past_history[self.max_tasks_to_run]['resource'].get_value()[self.change_for_one_task['resource']]
            change = False
            # same number of tasks and higher utilization
            if self.max_tasks_to_run == self.max_past_history['ntasks'] and current_rutil > self.max_past_history['resource']:
                change = True
            if not change:
                # is higher enough
                if current_rutil > self.max_past_history['resource'] and \
                    ((current_rutil - self.max_past_history['resource'])/(abs(self.max_tasks_to_run-self.max_past_history['ntasks'])) > \
                    config.TASK_UTIL_RATIO[self.change_for_one_task['resource']]):
                    change = True
            # smaller number of tasks and same resource utilization             
            if self.max_past_history['resource'] == 0:
                change = True
            elif not change and abs((current_rutil - self.max_past_history['resource'])/self.max_past_history['resource']) < 0.05 \
                and self.max_tasks_to_run < self.max_past_history['ntasks']:
                change = True
            if change:
                self.max_past_history['resource'] = current_rutil
                self.max_past_history['ntasks'] = self.max_tasks_to_run


    def compute_max_tasks(self):
        print self.qid, "Compute max taskx ##########################################"
	(diff, resource, resource_diff) = self.check_resource_deltas()
        all_resource = diff
	new_tasks = self.max_tasks_to_run
        ''' I'm at the beginning and I don't know anything....'''
        if len(self.past_changes) < 1 and resource_diff <= 0:
	    print "Resource diff is negative!!!! ", resource_diff
            return new_tasks
        if len(self.past_changes) < 1:
            resutil = self.past_history[self.max_tasks_to_run]['max']
            maxutil = resutil['cpu']
            self.change_for_one_task['resource'] = 'cpu'
            if resutil['network'] > maxutil:
                self.change_for_one_task['resource'] = 'network'
                maxutil = resutil['network']
            self.change_for_one_task['diff'] = maxutil
            self.past_changes.append((new_tasks, maxutil))
            print self.qid, "********", self.change_for_one_task, self.change_for_one_task['resource']
            print self.qid, "********", self.past_changes
            return new_tasks
        ''' look in history and update the util for the nb of tasks '''
        if len(self.past_changes) >= 2:
	    (direction, old_diff) = self.past_changes[-1]
        else:
	    (direction, old_diff) = self.past_changes[0]
	found_increase = False
	if len(self.past_changes) > 2:
            for (change, tmp_diff) in self.past_changes[:-1]:
                if change > 0:
                    found_increase = True
            ''' if I increase the nb of tasks and the utilization decreases :/ '''
        if direction !=0:
            new_tasks = self.increase_by_tcp(resource_diff/abs(direction), direction, resource, all_resource)
        else:
            new_tasks = new_tasks + 1 
        
        if len(self.past_changes) >= 3:
            if abs(new_tasks - self.max_tasks_to_run) == 1:
                if (self.past_changes[-2][0] == 1 or self.past_changes[-2][0] == 0) \
                    and (self.past_changes[-1][0] == -1 or \
                        self.past_changes[-1][0] == 0 ):
                        self.number_of_oscilations = self.number_of_oscilations + 1
            elif abs(new_tasks - self.max_tasks_to_run) > 1:
                self.number_of_oscilations = 0
    
        if self.number_of_oscilations >= self.jump_after:
            self.number_of_oscilations = 0
            new_tasks = self.max_tasks_to_run + random.randint(2, max(4, self.max_past_history['ntasks']-self.max_tasks_to_run))
        current_direction = new_tasks - self.max_tasks_to_run
        self.last_action = new_tasks - self.max_tasks_to_run
        # if I start decreasing I will increase in the future slower
        if self.last_action < 0 and self.external_change:
            self.external_change = False
        self.past_changes.append((self.last_action, resource_diff))
        if len(self.past_changes) > 4:
            self.past_changes.pop(0)
        print self.qid, "My past change queue is ", self.past_changes
        return new_tasks 
      

    def check_contention(self, diff, res, direction):
        contention = False
	current_rutil = self.past_history[self.max_tasks_to_run]['resource'].get_value()[self.change_for_one_task['resource']]
        max_diff = self.max_tasks_to_run - self.max_past_history['ntasks']
        min_change = min(config.TASK_UTIL_RATIO[self.change_for_one_task['resource']],\
            0.9*self.change_for_one_task['diff']) 
        if max_diff > 0:
            diff_change = (current_rutil - self.max_past_history['resource'])/max_diff
        else:
            diff_change = 0
        #print self.qid, min_change, abs(res)
        if abs(res) < min_change or (max_diff > 0 and diff_change < min_change): 
            contention = True
	return contention


    def increase_by_tcp(self, diff, direction, resource, all_resource):
        dir = 0
        new_tasks = self.max_tasks_to_run
        if direction < 0:
            dir = -1 # I decreased previously
        elif direction > 0:
            dir = 1 # I increased previously
        if abs(direction) > 0:
            res = all_resource[self.change_for_one_task['resource']]/abs(direction)
        else:
            new_tasks = new_tasks + 1
            return new_tasks
        if dir == 0:
            return new_tasks + 1
        # If there is contention, decrease the nb of tasks, else increase it
        if self.check_contention(diff, res, direction):
            self.is_contention = True    
            new_direction = -1
            if self.max_tasks_to_run > 1 and (self.max_tasks_to_run) in self.ntasks_before_contention:
                self.ntasks_before_contention.remove(self.max_tasks_to_run)
                self.ntasks_before_contention.sort(reverse=True)
	    if(self.max_tasks_to_run not in self.bad_points):
		self.bad_points.append(self.max_tasks_to_run)
        else:
            new_direction = 1
	    self.bad_points.remove(self.max_tasks_to_run)
            # todo : I need faster lookup here!
            if new_tasks not in self.ntasks_before_contention:
                self.ntasks_before_contention.append(new_tasks)
            self.ntasks_before_contention.sort(reverse=True)
        if new_direction != dir:
            self.same_dir = 0
            self.tasks_increase = 0
        if new_direction < 0:
            new_tasks = max(1,self.max_tasks_to_run - max(1, int(direction/2)))
            return new_tasks
        else:
            # continue in the same direction
            self.same_dir = self.same_dir + 1
        if  new_direction == 1:
            # increase first incrementally, then exponentially
            if self.same_dir > self.start_exp_after_increase:
                current_rutil = self.past_history[self.max_tasks_to_run]['resource'].get_value()[self.change_for_one_task['resource']]
                self.tasks_increase = int(math.ceil(self.tasks_increase * (1+abs(diff)/self.change_for_one_task['diff'])))
            else:
                self.tasks_increase = self.tasks_increase + 1
        elif new_direction == -1:
            if res == 0:
                self.tasks_increase = 1
            else:
                self.tasks_increase = self.tasks_increase + 1 
        if new_direction == 1:
            # if it's the first time we increase, we are more agressive
            new_tasks_tmp = new_tasks + self.tasks_increase
            # check here if we increase to a nb of tasks that we've already visited and it was bad
            exp = 1
            while self.past_history.get(new_tasks_tmp) != None and new_tasks_tmp > new_tasks + 1:
                if new_tasks_tmp in self.bad_points:
                    new_tasks_tmp = new_tasks + max(1, int(new_tasks/(2**exp)))
                    exp = exp + 1
                else:
                    break
            new_tasks = new_tasks_tmp
        elif new_direction == -1:
            tmp_ntasks = self.ntasks_before_contention[0]
            index = 1
            while tmp_ntasks >= self.max_tasks_to_run and index < len(self.ntasks_before_contention):
                tmp_ntasks = self.ntasks_before_contention[index]
                index = index + 1
            if self.same_dir > self.start_exp_after_decrease:
                new_tasks = max(1, self.max_tasks_to_run - max(1, int((self.max_tasks_to_run - tmp_ntasks)/2)))
                self.same_dir = 0
                self.tasks_increase = 0
            else:
                new_tasks = max(1,new_tasks - self.tasks_increase)
        return new_tasks


   
class ProfilingStates(object):
    state = {'wait': 0, 'running' : 1, 'profiling' : 2}
    def __init__(self):
	pass

class Dynamic4NodeScheduler(NodeScheduler):
    
    def __init__(self):
        NodeScheduler.__init__(self)
        self.is_profiling = True
        #self.tqueue.start()
        self.last_action = 0
        self.resource_delta_history = []
        ''' variables used by the improved policy '''
	self.past_value_w = 0.3
	self.past_history_max_len = 10 
	self.profiling = False
	self.c = config.TASK_UTIL_RATIO
	self.current_task = ""
	self.restart = False
	self.adapt_queues = []
	self.adapt_queues_lock = threading.Lock()
	self.state = ProfilingStates.state['running']
	self.profiled_queues = []
	self.pending_queues = []

    def process_task(self, task):
        new_task = super(Dynamic4NodeScheduler, self).process_task(task)
        qid = hashlib.sha1(task.encode()).hexdigest()
	if new_task:
	    are_queues_to_wait = False
	    are_queues_profiling = False
	    if len(self.adapt_queues) > 0:
		for worker in self.adapt_queues:
		    if worker.is_profiling:
			are_queues_profiling = True
		    elif worker.tqueue.get_size() != 0 or self.max_tasks_to_run[worker.qid] >= 0:
			are_queues_to_wait = True
	    for worker in self.adapt_queues:
		if self.max_tasks_to_run[worker.qid] != -1 and not worker.is_profiling:
		    #are_queues_to_wait = True
		    worker.past_max_tasks = worker.max_tasks_to_run
		    worker.max_tasks_to_run = 0
		    worker.tqueue.resize(0, [])
		    self.max_tasks_to_run[worker.qid] = 0
	    qid = hashlib.sha1(task.encode()).hexdigest()
	    print "Detected new task ", task, " will create an adaptation instance with id ", qid
	    try:
		new_worker = Adapt(qid, self.queue_data[qid]['tpool'])
	    except:
		traceback.print_exc()
	    self.adapt_queues_lock.acquire()
	    self.adapt_queues.append(new_worker) 
	    self.adapt_queues_lock.release()
	    self.empty_task_data()
	    print "After wait for adapt_queues_lock"
	    self.restart = False
	    if not are_queues_to_wait and not are_queues_profiling:
		print "There are no queues to wait for, will profile ", new_worker.qid
	        self.state = ProfilingStates.state['profiling']
                for worker in self.adapt_queues:
                    worker.past_history = {}
                self.resource_delta_history = []
	        new_worker.max_tasks_to_run = 1
		new_worker.tqueue.resize(1, [])
		new_worker.is_profiling = True
		self.max_tasks_to_run[new_worker.qid] = 1
	    elif not are_queues_profiling:
		 self.state = ProfilingStates.state['wait']
	elif self.max_tasks_to_run[qid] == -1:
	    print "Detected task from old disabled queue!"
	    for worker in self.adapt_queues:
		if worker.qid == qid:
	    	    myworker = worker
		    break
	    if myworker.tqueue.tasks.empty():
		are_queues_to_wait = False
                are_queues_profiling = False
                if len(self.adapt_queues) > 0:
                    for worker in self.adapt_queues:
                        if worker.is_profiling:
                            are_queues_profiling = True
                        elif worker.tqueue.get_size() != 0 or self.max_tasks_to_run[worker.qid] >= 0:
                            are_queues_to_wait = True

		if not are_queues_to_wait and not are_queues_profiling:
		    myworker.max_tasks_to_run = 1
                    myworker.tqueue.resize(1, [])
                    myworker.is_profiling = True
                    self.max_tasks_to_run[myworker.qid] = 1
		elif not are_queues_profiling:
                    self.state = ProfilingStates.state['wait']
	    resource = self.queue_data[qid]['resource']
	    skip = False
	    for worker in self.adapt_queues:
                if self.queue_data[worker.qid]['resource'] == resource and \
                    worker.qid != myworker.qid and self.max_tasks_to_run[worker.qid] != -1:
		    self.max_tasks_to_run[qid] = 0
		    myworker.max_tasks_to_run = 0
		    myworker.tqueue.resize(0, [])
		    self.pending_queues.append(myworker)
		    skip = True
	    if not skip:
	        self.max_tasks_to_run[qid] = self.queue_data[qid]['tpool'].get_size()
	return new_task


    def read_resource_utilization(self):
	past_maximum = 0
	if config.UTILIZATION_METRIC == 'maximum':
	    (past_avg, past_maximum, past_std) = self.monitor_thread.get_average_utilization()
	elif config.UTILIZATION_METRIC == 'average':
	    (past_maximum, past_avg, past_std) = self.monitor_thread.get_average_utilization()
	elif config.UTILIZATION_METRIC == 'median':
	    past_maximum = self.monitor_thread.get_median_utilization()
	elif config.UTILIZATION_METRIC == 'histogram':
	    past_maximum = self.monitor_thread.get_utilization_by_histogram()

	task_data = self.monitor_thread.get_task_data()
	total_util = {'cpu': 0, 'memory': 0, 'cpu_sys': 0}
	for task in task_data:
	    for data in task_data[task]:
		total_util['cpu'] = total_util['cpu'] + data[0][0]/len(task_data[task])
		total_util['cpu_sys'] = total_util['cpu_sys'] + data[0][1]/len(task_data[task])
		total_util['memory'] = total_util['memory'] + data[1]/len(task_data[task])

	return (past_maximum, total_util)


    def resize_long_queue(self, worker, new_tasks):
	csize = worker.tqueue.get_size()
	if csize - new_tasks < 0:
	    worker.tqueue.resize(new_tasks, [])
	    return
	ctime = time.time()
	to_delete = []
	for tid in self.task_data:
	    task_data[tid]['lock'].acquire()
	    try:
	        cprocess = task_data[tid]['proc']
		wall_time = ctime - cprocess.create_time()
		if wall_time < 2*config.WAITTIME:
		    to_delete.append(tid)
	    except:
		continue	
	    task_data[tid]['lock'].release()
	to_delete2 = []
	if len(to_delete) > csize - new_tasks:
	    for taskid in range(0, csize - new_tasks):
	        to_delete2.append(to_delete[taskid])
	worker.tqueue.resize(new_tasks, to_delete2)
	for tid in to_delete2:
	    task_data[tid]['lock'].acquire()
	    try:
		cprocess = task_data[tid]['proc']
		cprocess.kill()
	    except:
		# hopefully the process does not exist anymore...
		task_data[tid]['lock'].release()
		continue
	    # re-queue the task
	    task = task_data[tid]['ctask']
	    worker.tqueue.add_task(run_task, task)
	    task_data[tid]['lock'].release()

    def change_work_queue(self, nrunning, nrunning_past, avg_time, avg_cpu):
        ''' queue is empty - I don't do anything; if I have to decrease ntasks wait '''
	print "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Changing work queue size and profiling !!!! %%%%%%%%%%%%%%%%%%"
	need_wait = False
	max_tasks_to_run = 0
	for worker in self.adapt_queues:
	    if worker.last_action < 0:
		need_wait = True
	    max_tasks_to_run = max_tasks_to_run + worker.max_tasks_to_run
	if need_wait and nrunning > max_tasks_to_run:
	    print "Waiting for tasks to finish....."
	    self.logger.error("Some tasks from the previous scheduling period did not finish!")
	    return
	resources_started = []
        to_delete = []
	print "Pending queues ", self.pending_queues
        for queue in self.max_tasks_to_run:
            if self.max_tasks_to_run[queue] == -1:
		print "Queue ", queue, "is empty!"
                resource = self.queue_data[queue]['resource']
                for worker in self.pending_queues:
		    print resource, self.queue_data[worker.qid]['resource'], resources_started
                    if self.queue_data[worker.qid]['resource'] == resource and \
                            self.queue_data[worker.qid]['resource'] not in resources_started:
                        # now this queue is ready to be processed
			new_size = int(self.capacity['cores'])
			worker.tqueue.resize(new_size, [])
                        worker.max_tasks_to_run = new_size
                        self.max_tasks_to_run[worker.qid] = new_size
                        print "I change max_task_to_run to 1 for queue ", worker.qid
			resources_started.append(resource)
                        to_delete.append(worker)
        for worker in to_delete:
            self.pending_queues.remove(worker)
	resources_started = []
	to_delete = []
	for worker in self.pending_queues:
	    skip = False
	    # TODO - resource is unassigned
	    for queue in self.max_tasks_to_run:
		if self.max_tasks_to_run[queue] > 0 and worker.qid != queue:
		    resource = self.queue_data[queue]['resource']
		    if resource == self.queue_data[worker.qid]['resource'] or \
			self.queue_data[worker.qid]['resource'] in resources_started:
			skip = True
	    if skip:
		continue
	    new_size = int(self.capacity['cores'])
            worker.tqueue.resize(new_size, [])
            worker.max_tasks_to_run = new_size
            self.max_tasks_to_run[worker.qid] = new_size
            print "I change max_task_to_run to 1 for queue ", worker.qid
            resources_started.append(self.queue_data[worker.qid]['resource'])
            to_delete.append(worker)
	for worker in to_delete:
            self.pending_queues.remove(worker)
	if self.state == ProfilingStates.state['wait']:
	    print "I have new task so I wait for running tasks to finish ", nrunning
	    if nrunning > 0:
		return
	    print "I move on profiling worker!!"
	    self.state = ProfilingStates.state['profiling']
	    for worker in self.adapt_queues:
            	worker.past_history = {}
	    self.resource_delta_history = []	

	if self.state == ProfilingStates.state['profiling']:
	    need_profiling = False
	    # check if there are queues in profiling state...
	    queue_profiling = False
	    worker_to_profile = None
	    for worker in self.adapt_queues:
		if worker.is_profiling:
		    print "Worker ", worker.qid, " is profiling!!"
		    queue_profiling = True
		    worker_to_profile = worker
		    self.resource_delta_history = []
		    if worker.max_tasks_to_run == 0:
			worker.max_tasks_to_run = 1
                        self.max_tasks_to_run[worker.qid] = 1
                        worker.tqueue.resize(1, [])
		    break
		else:
		    print "Worker ", worker.qid, " is not profiling!"
	    workers_to_profile = 0
	    print "I pick next queue to profile!!"
	    # pick next queue to profile
	    for worker in self.adapt_queues:
		if len(worker.past_changes) < 1 and not worker.is_profiling and worker.tqueue.tasks.qsize() > 0:
		    workers_to_profile = workers_to_profile + 1
		    need_profiling = True
	    	    if worker_to_profile == None:
			worker_to_profile = worker
			worker.is_profiling = True
	    # if there is a queue that is profiling check if it finished:
	    if queue_profiling:
		print "Checking if finished profiling!!!"
		if len(worker_to_profile.past_changes) == 1:
		    print "Stop profiling ", worker_to_profile.qid, "now, checking for contention!"
		    worker_to_profile.is_profiling = False
		    self.queue_data[worker_to_profile.qid]['resource'] = worker_to_profile.change_for_one_task['resource']
		    is_contention = False
		    # how do I decide if my queue continues running?
		    for worker1 in self.adapt_queues:
			if worker1.qid == worker_to_profile.qid:
			    break
			# check contention with others
			if worker_to_profile.change_for_one_task['resource'] == worker1.change_for_one_task['resource'] and \
				self.max_tasks_to_run[worker_to_profile.qid] != -1:
			    print "I found contention for ", worker_to_profile.change_for_one_task['resource']
			    is_contention = True
			    break
		    print "Resizing queue to 0!!!"
		    worker_to_profile.tqueue.resize(0, [])
		    worker_to_profile.max_tasks_to_run = 0
		    self.max_tasks_to_run[worker_to_profile.qid] = 0
		    if not is_contention:
			# we will start them after we finished profiling all queues
			self.profiled_queues.append(worker_to_profile)
		    else:
			self.pending_queues.append(worker_to_profile)
		    if workers_to_profile == 0:
		        queue_profiling = False
			print "There are no more queues to profile"
	   	    else:
			self.state = ProfilingStates.state['wait']
			for worker in self.adapt_queues:
                    	    if len(worker.past_changes) < 1 and not worker.is_profiling:
				worker.is_profiling = True
				break
			return
		elif worker_to_profile.tqueue.tasks.empty() and nrunning == 0:
		    worker_to_profile.is_profiling = False
		    worker_to_profile.tqueue.resize(0, [])
		    worker_to_profile.max_tasks_to_run = 0
                    self.max_tasks_to_run[worker_to_profile.qid] = 0
		    if workers_to_profile == 0:
                        queue_profiling = False
		    return
	    if not need_profiling and not queue_profiling:
		self.state = ProfilingStates.state['running']
		for worker in self.adapt_queues:
		    worker.is_profiling = False
		    if worker not in self.profiled_queues and worker not in self.pending_queues:
			worker.restored = True
			print "Restoring running queue ", worker.qid
			worker.tqueue.resize(worker.past_max_tasks, [])
			worker.max_tasks_to_run = worker.past_max_tasks
			self.max_tasks_to_run[worker.qid] = worker.past_max_tasks
		    elif worker in self.profiled_queues:
			print "Restoring profiled queue ", worker.qid
			worker.tqueue.resize(int(self.capacity['cores']), [])
			worker.restored = True
			worker.max_tasks_to_run = int(self.capacity['cores'])  
			self.max_tasks_to_run[worker.qid] = int(self.capacity['cores'])
		self.profiled_queues = []
	# update resource utilization here
        (past_maximum, total_util) = self.read_resource_utilization()
        #print config.UTILIZATION_METRIC, past_maximum
        actual_value = {}
        actual_value['memory'] = past_maximum['memory']
        actual_value['network'] = past_maximum['network']
        actual_value['cpu'] = past_maximum['cpu'] #- total_util['cpu']
        actual_value['cpu_io'] = 0 #100*past_maximum['cpu_io']/cpu_sum
        ''' here I remove the memory consumed by memfs '''
        if actual_value['cpu_io'] > 0:
            actual_value['disk'] = actual_value['cpu_io']/actual_value['cpu']
        else:
            actual_value['disk'] = 0
        print "Total utilization: ", actual_value
        for worker in self.adapt_queues:
            # update the history for each task queue
            if worker.max_tasks_to_run != 0:
                worker.update_resource_history(self.past_history_max_len, self.past_value_w, actual_value)
                self.logger.info("Reconf utilization: %s %s %s" %(
                    worker.past_history[worker.max_tasks_to_run]['resource'].get_value()['cpu'], \
                    worker.past_history[worker.max_tasks_to_run]['resource'].get_value()['memory'], \
                    worker.past_history[worker.max_tasks_to_run]['resource'].get_value()['network']))
		try:
		     current_rutil = worker.past_history[worker.max_tasks_to_run]['resource'].get_value()
		except:
		     current_rutil = actual_value
		worker.add_to_history(current_rutil)
	
        if self.get_total_queue_size() == 0 and nrunning == 0:
	    self.logger.error("Current queue size is 0!")
	    return
	# we adapt only when we have tasks in queues
	''' policy follows here '''
	# if the nb of running tasks is smaller than the max nb of slots than the measurements
	# might be wrong, so we skip the adaptation ; actually it will be nice to 
	# set the nb of slots to the nb of tasks to avoid misunderstandings
	max_running_tasks = 0
	for x in self.adapt_queues:
	    if self.max_tasks_to_run[x.qid] > 0:
	        max_running_tasks = max_running_tasks + self.max_tasks_to_run[x.qid]
	counter = 0
	is_majority = False
	for c in nrunning_past:
	    if c >= max_running_tasks:
		counter = counter + 1
	print counter, int(len(nrunning_past)/2)
	if counter > int(len(nrunning_past)/2):
	    is_majority = True
	print "I adapt the workers!"
	for worker in self.adapt_queues:
	    if worker.restored:
		worker.restored = False
		print "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Continue to the next worker......"
		continue
	    print "Worker max tasks to run : ", worker.max_tasks_to_run
	    new_tasks = worker.max_tasks_to_run
	    print avg_time, worker.tqueue.get_size(), worker.max_tasks_to_run, is_majority
	    if len(avg_time) and  worker.tqueue.get_size() > 0 and \
		    worker.max_tasks_to_run > 0 and self.max_tasks_to_run[worker.qid] > 0 \
		    and is_majority:
		new_tasks = worker.compute_max_tasks()
		worker.max_tasks_to_run = new_tasks
	    	if worker.max_tasks_to_run == worker.max_past_history['ntasks'] and \
		    worker.change_for_one_task['resource'] != '':
		    worker.max_past_history['resource'] = actual_value[worker.change_for_one_task['resource']]
		if worker.tqueue.get_size() > new_tasks:
		    if self.queue_data[worker.qid]['type'] == 'long':
			self.resize_long_queue(worker, new_tasks)
		    else:
			worker.tqueue.resize(new_tasks, [])
		else:
	    	    worker.tqueue.resize(new_tasks, [])
	    	self.max_tasks_to_run[worker.qid] = new_tasks
	self.current_ntasks = sum(x.max_tasks_to_run for x in self.adapt_queues)
