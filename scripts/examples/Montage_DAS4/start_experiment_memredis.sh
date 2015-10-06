#!/bin/sh

# first parameter: how many memcached nodes I want to have from the total

for line in `cat ~/memcached_servers.txt`
do
	node=`echo $line | cut -d ':' -f 1`
        ssh $node "pkill -9 redis-server"
done


for line in `cat ~/memcached_servers.txt`
do
	node=`echo $line | cut -d ':' -f 1`
	ssh $node "mkdir /local/$USER"
	scp ~/memcached_servers.txt $node:/local/$USER/ip_redis_table.txt
	scp ./redis-common.conf $node:/local/$USER/
	ssh -n -f $node "sh -c 'nohup /home/$USER/bin/redis-server /local/$USER/redis-common.conf  > /dev/null 2>&1 &'"
done

for node in `cat ./memfs_servers.txt`
do
        echo "Mounting MemFS on : $node"
        scp ~/memcached_servers.txt $node:/local/$USER/ip_redis_table.txt
	ssh $node "mkdir /local/$USER/memfs2 ; /home/$USER/Weasel/scripts/examples/blast/mount_fuse.sh /local/$USER/memfs2"
	ssh $node "mkdir /local/$USER/memfs3 ; /home/$USER/Weasel/scripts/examples/blast/mount_fuse.sh /local/$USER/memfs3"
	ssh $node "mkdir /local/$USER/memfs4 ; /home/$USER/Weasel/scripts/examples/blast/mount_fuse.sh /local/$USER/memfs4"
done

# this starts the scheduler
~/Weasel/scripts/run_weasel.sh weasel_ips

main_node=`tail -n 2 weasel_ips | head -n 1`

./Montage_multiqueue.sh /local/$USER/memfs3/ /var/scratch/$USER/montage_6
