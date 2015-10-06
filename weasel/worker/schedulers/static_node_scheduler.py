import weasel.etc.config as config
from weasel.utils.threads import *
from weasel.worker.node_scheduler import NodeScheduler
import hashlib

class StaticNodeScheduler(NodeScheduler):
    def __init__(self):
        NodeScheduler.__init__(self)
        self.is_profiling = False
        self.static_max_tasks_to_run = min(int(self.capacity['cores'] / config.SLOT_SIZE['cpu']), \
               int(self.capacity['memory'] / config.SLOT_SIZE['memory']))
	self.logger.info("Initializing thread pool with %s workers" % self.static_max_tasks_to_run)
	self.logger.info("Scheduler policy is %s" % config.POLICY)
	# we make a random hash for our queue
	self.random_hash = hashlib.sha1(b'/tmp/Weasel/bin/local_resourcemanager').hexdigest()


    def add_task_to_queues(self, tasks):
	# so fot the static scheduler we don't care because we have only one queue
	self.queue_data_lock.acquire()
	if len(self.queue_data) < 1:
	    self.queue_data[self.random_hash] = {'qid': self.random_hash,
		    'asked':0, 'recv':0,
		    'tpool': ThreadPool(1, self.task_data),
		    'resourcev': [] } # this contains the resource demand ch of the tasks
	    tpool = self.queue_data[self.random_hash]['tpool']
	    self.max_tasks_to_run[self.random_hash] = self.static_max_tasks_to_run
	    tpool.set_size(self.static_max_tasks_to_run)
	    tpool.start()
	self.queue_data_lock.release()
	self.max_tasks_to_run[self.random_hash] = self.static_max_tasks_to_run
	for task in tasks['tasks']:
	    #print task['exec'], task['params']
	    ''' here: if the task does not exist in my history:
		shrink the pool at 1 task and enter the profiling mode
		profiling mode = record resource util for the first 10 tasks '''
	    new_task = self.process_task(task['exec'])
	    task_hash = hashlib.sha1(task['exec'].encode()).hexdigest()
	    if new_task:
		self.max_tasks_to_run[task_hash] = self.static_max_tasks_to_run
	    self.has_new_task = self.has_new_task | new_task
	    task['qid'] = task_hash
	    self.add_task_to_queue(self.queue_data[self.random_hash]['tpool'], task)

