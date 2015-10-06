#!/bin/sh

# this sets up the nodes with infiniband

# first parameter: how many memcached nodes I want to have from the total

# this starts the in-memory file-system
for line in `cat ~/memcached_servers.txt`
do
        ssh $line "pkill -9 memcached"
done

for line in `cat ./memfs_servers.txt`
do
	echo "Mounting MemFS on : $line"
	ssh $line "mkdir /tmp/memfs2 ; /home/$USER/Weasel/scripts/examples/Montage_DAS4/mount_fuse.sh /tmp/memfs2"
	ssh $line "mkdir /tmp/memfs3 ; /home/$USER/Weasel/scripts/examples/Montage_DAS4/mount_fuse.sh /tmp/memfs3"
	ssh $line "mkdir /tmp/memfs4 ; /home/$USER/Weasel/scripts/examples/Montage_DAS4/mount_fuse.sh /tmp/memfs4"
done
./remote_memcached.sh ~/memcached_servers.txt

# this starts the scheduler
~/Weasel/scripts/run_weasel.sh weasel_ips

main_node=`tail -n 2 weasel_ips | head -n 1`
