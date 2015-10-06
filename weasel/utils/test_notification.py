#!/bin/python

import sys
import os

from merkat.drivers.notification.notification import *


def usage():
         print """
         Usage: test_notification [start|stop|send] [OPTIONS]
                -f --file  file path to be passed to the create/submit command
                -i --id    process id if the user wants to delete the platform
                -p --parameter name of the configuration parameter to edit
                -v --value     new value of the configuration parameter
         """
         sys.exit(2)



if len(sys.argv) < 2:
    print "Need to specify Client/Server option"
    sys.exit(1)
    
type = sys.argv[1]

if type == "server":
    zmq_context = get_context()
    zmq_frontend = ZmqServerTask(zmq_context, config.ZMQ_FRONTEND_PORT, \
                                       msg_process_callback)