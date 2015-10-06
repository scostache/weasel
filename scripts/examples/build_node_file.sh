#!/bin/sh

node_file=$1

IPS=`preserve -llist | grep $USER | head -n 1 | tr -d '\t' | awk -F"node" '{ for(i = 0; i < 100; i++) { if ($i) print $i; } }' | grep -v $USER |  sed 's/0*//' | tr -d '\n'`
rm all_ib_ips all_eth0_ips
rm $node_file
# we use infiniband
for ip in $IPS
do
	echo 10.149.0.$ip:6379 >> all_ib_ips
	echo 10.141.0.$ip >> all_eth0_ips
done
if [ $2 -eq 0 ]; then
	cat all_ib_ips | tail -n+2 > ~/memcached_servers.txt
	cat all_eth0_ips > $node_file
	cat all_eth0_ips > memfs_servers.txt
	exit 0
fi
	
cat all_ib_ips | tail -n $2 > ~/memcached_servers.txt
ip_nb=`cat all_ib_ips | wc -l`
memfs_ips=$(( $ip_nb - $2 ))
#cat all_eth0_ips > weasel_ips
#cat all_eth0_ips > memfs_servers.txt
cat all_eth0_ips | head -n $memfs_ips > memfs_servers.txt
cat all_eth0_ips | head -n $memfs_ips > $node_file
