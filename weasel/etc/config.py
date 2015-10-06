"""
   General configuration
"""
import sys
sys.setrecursionlimit(4000)

''' where to save a log for everything '''
LOGDIR='/local/user/logs/weasel/'

''' DEBUG, INFO, ERROR '''
LOG_LEVEL='DEBUG'

''' scheduler address '''
SCHEDULER="10.141.0.6"

''' scheduler ZMQ port '''
ZMQ_SCHEDULER_PORT = 5555

''' how many seconds to sleep between 2 scheduling periods '''
WAITTIME = 4

''' how many seconds to sleep between 2 utilization readings (make it under 1 sec for more accurate readings)'''
MONITOR_PERIOD = 0.5

T_LONG = 60

''' the policy the worker will use to ask for tasks:
static - the worker asks a fixed nb of tasks based on slot size 
dynamic4: - the worker adds tasks until it detects contention for at least one resource (my algorithm)
'''

POLICY = 'dynamic4'

'''
utilization metric to use (how utilization is computed from the polled samples):
- average, maximum, median, histogram (aproximates median somehow)
'''
UTILIZATION_METRIC = "histogram"

''' thresholds for the dynamic policy '''

DYNAMIC_TLOW = {'cpu': 0.85, 'memory': 0.9, 'network': 0.55}

DYNAMIC_THIGH = {'cpu': 0.95, 'memory': 0.99, 'network':0.65}

''' deprecated?'''
PROFILE_NTASKS = 10

''' used for the static policy '''
SLOT_SIZE = {'cpu': 1, 'memory' : 900}

''' used by dynamic4 policy; contention is detected when resource utilization increases less than the utilization
of one task divided by a factor TASK_UTIL_RATIO
'''
TASK_UTIL_RATIO = {'cpu' : 0.5, 'memory' : 0.01, 'network' : 1, 'disk' : 0.1, 'cpu_io': 0.1}

''' to track the utilization of the filesystem '''
MONITOR_MEMFS = True
''' if I want to monitor MEMFS processes, I need their pids '''
''' memcached process '''
MEMFS_PROCESS=-1
''' memfs process '''
MEMFS2_PROCESS=14575

''' this parameter defines the max bandwith in the cluster (Kb);
it is needed to avoid benchmarking the nodes '''
BANDWITH= 1048576
