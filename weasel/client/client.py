import socket
import uuid
import pickle
import hashlib
from weasel.etc.config import *
from weasel.utils.notification import *


'''
simple CLI client to put tasks in a queue and sleep until they are finished, and get the answer at the end, with timing
functionality
'''

'''
- queue
- status
- kill
'''


class Client(object):

    def __init__(self, address):
        self.address = address
        self.identity = 'client-' + socket.gethostbyname(socket.gethostname())

    def queue(self, task):
        ''' queues a command '''
        app_id = uuid.uuid1()
        context = zmq.Context()
        mysocket = tmp_socket(context, zmq.DEALER, self.identity,
                              SCHEDULER + ":" + str(ZMQ_SCHEDULER_PORT))
        message = [str(app_id), '', PROTOCOL_HEADERS['CLIENT'], 'queue', task]
        send_message(mysocket, message)
        mysocket.close()
        context.term()
        print "Your Application ID is : ", app_id

    def queue_file(self, file):
        ''' queues all commands from a given file '''
        context = zmq.Context()
        mysocket = tmp_socket(context, zmq.DEALER, self.identity,
                              SCHEDULER + ":" + str(ZMQ_SCHEDULER_PORT))
        app_id = 0
	f = open(file, 'r')
        line = f.readline()
        string_to_send = {}
        max_to_send = 100
	current_to_send = 0
	task_list = []
	previous_hash = None
	current_hash = None
	while line:
            tokens = line.split('=')
            inputval = tokens[0]
	    try:
                tmpval = tokens[1][:-1]
            except:
                tmpval = []
            if inputval == 'exec':
                paramval = tokens[1]
                for token in tokens[2:]:
                    paramval = paramval + '=' + token
                tmpval = paramval[:-1]
            string_to_send[inputval] = tmpval
	    line = f.readline()
            if inputval == 'outputs':
		if previous_hash == None:
		    previous_hash = hashlib.sha1(string_to_send['exec']).hexdigest()
		current_hash = hashlib.sha1(string_to_send['exec']).hexdigest()
		if current_hash == previous_hash:
		    task_list.append(string_to_send.copy())
		#print "Sending tasks for appId: ", previous_hash, "task exec: ", string_to_send['exec']
		if current_to_send == max_to_send or not line or current_hash != previous_hash:
		    current_to_send = 0
                    data = pickle.dumps(task_list)
                    app_id = previous_hash #hashlib.sha1(string_to_send['exec']).hexdigest()
		    print "Sending tasks for appId: ", previous_hash, "task exec: ", task_list
                    message = [
                    	str(app_id),
                    	'',
                    	PROTOCOL_HEADERS['CLIENT'],
                    	'queue',
                    	data]
                    send_message(mysocket, message)
            	    task_list = []
		    if current_hash != previous_hash:
			task_list.append(string_to_send)
			previous_hash = current_hash
		string_to_send = {}
		current_to_send = current_to_send + 1
        f.close()
        mysocket.close()
        context.term()
        print "Your Application ID is : ", app_id

    def status(self, app_id):
        ''' gets status about the tasks that were queued: how many are left (?) '''
        context = zmq.Context()
        mysocket = tmp_socket(context, zmq.REQ, self.identity + '-sync',
                              SCHEDULER + ":" + str(ZMQ_SCHEDULER_PORT))
        message = [str(app_id), PROTOCOL_HEADERS['CLIENT'], 'status']
        send_message(mysocket, message)
        reply = get_reply(mysocket)
        print "I received status: ", reply
        mysocket.close()
        context.term()

    def waitall(self, app_id):
        ''' the same but this time I will get my reply when all tasks finish! '''
        print "Opening Socket...."
        context = zmq.Context()
        mysocket = tmp_socket(context, zmq.REQ, self.identity + '-sync',
                              SCHEDULER + ":" + str(ZMQ_SCHEDULER_PORT))
        print "Sending message to wait for tasks to finish...."
        message = [str(app_id), PROTOCOL_HEADERS['CLIENT'], 'wait']
        send_message(mysocket, message)
        print "Waiting for the answer! ....."
        reply = get_reply(mysocket)
        mysocket.close()
        context.term()
        print "All tasks finished executing!"

    def clear(self, app_id):
        context = zmq.Context()
        mysocket = tmp_socket(context, zmq.REQ, self.identity + '-sync',
                              SCHEDULER + ":" + str(ZMQ_SCHEDULER_PORT))
        message = [str(app_id), PROTOCOL_HEADERS['CLIENT'], 'clear']
        send_message(mysocket, message)
        mysocket.close()
        context.term()

    def kill(self, worker_id):
        context = zmq.Context()
        mysocket = tmp_socket(context, zmq.REQ, self.identity + '-sync',
                              SCHEDULER + ":" + str(ZMQ_SCHEDULER_PORT))
        message = [
            str(app_id),
            PROTOCOL_HEADERS['CLIENT'],
            'delete',
            worker_id]
        send_message(mysocket, message)
        mysocket.close()
        context.term()
