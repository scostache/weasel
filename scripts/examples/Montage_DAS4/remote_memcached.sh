#! /bin/bash

for ip in `cat $1`
do
echo $ip
ssh -n -f $ip "sh -c 'nohup memcached -m 16000 -t 4 -c 40000 -I 4m  > /dev/null 2>&1 &'"
done

