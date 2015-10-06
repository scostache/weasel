#!/bin/sh

ip_file=$1

main_node=`head -n 1 $ip_file`

~/Weasel/scripts/stop_weasel.sh $ip_file $2

for line in `cat $ip_file`
do
	echo "killing everything on node $line"
        ssh $line "pkill -9 memcached; pkill -9 redis-server"
	ssh $line "fusermount -u /tmp/memfs2"
        #ssh $line "fusermount -u /tmp/memfs3"
	#ssh $line "fusermount -u /tmp/memfs4"
	ssh $line "pkill -9 memcachefs"
	ssh $line "rm -rf /tmp/memfs2"
	#ssh $line "rm -rf /tmp/memfs3"
	#ssh $line "rm -rf /tmp/memfs4"
done

