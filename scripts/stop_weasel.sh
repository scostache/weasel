#!/bin/sh

node_file=$1
log_name=$2

for node in `cat $node_file`; do
	ssh $node "pkill -9 local_resourcem"
done

head=`head -n 1 $node_file`
ssh $head "pkill -9 resourcemng"

now=`date +'%s'`
mkdir $log_name

for node in `cat $node_file`; do
	mkdir $log_name/$node
	scp -r $node:/local/$USER/logs ./$log_name/$node/ > /dev/null
	ssh $node "rm -rf /local/$USER/Weasel ; rm -r /local/$USER/logs/"
done
