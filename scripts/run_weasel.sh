#!/bin/sh

host_file=$1

echo "I run Weasel on nodes: "
cat $host_file
# start the scheduler on the first node
# start the workers on all the other nodes

head=`head -n 1 $host_file`
sed -i "s/SCHEDULER\=.*/SCHEDULER\=\"$head\"/" ~/Weasel/weasel/etc/config.py

for node in `cat $host_file`; do
    ssh $node "mkdir -p /local/$USER/logs ; mkdir /local/$USER/logs/weasel"
    memcached_pid=`ssh $node "pidof memcached | cut -d ' ' -f 1"`
    if [ -z "$memcached_pid" ]; then
	memcached_pid=-1
    fi
    memcachefs_pid=`ssh $node "pidof memcachefs | cut -d ' ' -f 1"`
    if [ -z "$memcachefs_pid" ]; then
	memcachefs_pid=-1
    fi
    sed -i "s/MEMFS_PROCESS\=.*/MEMFS_PROCESS\=$memcached_pid/" ~/Weasel/weasel/etc/config.py
    sed -i "s/MEMFS2_PROCESS\=.*/MEMFS2_PROCESS\=$memcachefs_pid/" ~/Weasel/weasel/etc/config.py
    scp -r ~/Weasel $node:/local/$USER/ > /dev/null
done

head_node=`head -n 1 $host_file`
echo "Starting master on node $head_node"
# start the master
ssh -f $head_node "sh -c 'export PYTHONPATH=$PYTHONPATH:/home/$USER/pyzmq-14.0.1/build/lib.linux-x86_64-2.6/:/local/$USER/Weasel/ ; nohup /local/$USER/Weasel/bin/resourcemng < /dev/null 2>&1 > /local/$USER/logs/resourcemng_nohup.out &'"
# start the workers
for node in `cat $host_file | tail -n+2` ; do
    echo "Starting worker on node $node"
    ssh -f $node "sh -c 'export PYTHONPATH=$PYTHONPATH:/home/$USER/pyzmq-14.0.1/build/lib.linux-x86_64-2.6/:/local/$USER/Weasel/ ; nohup /local/$USER/Weasel/bin/local_resourcemng < /dev/null 2>&1 > /local/$USER/logs/local_resourcemng_nohup.out &'"
done
