#!/bin/sh

# $1 number of repetitions; $2 log path

nb_repetition=$1

i=0
rm blast_stats
mkdir logs_weasel_memfs 0
while [ $i -lt $nb_repetition ]; do
	echo "Repeating experiment for the $i-th time"
	./start_experiment.sh 
	echo "Done with the $i-th experiment...."
	# here I take the logs from one node (the last one)
	node=`tail -n 1 weasel_ips`
	cdate=`date +%s`
	../kill_experiment.sh weasel_ips logs_weasel_memfs/logs_$cdate
	../kill_experiment.sh weasel_ips 0
	rm -r 0
	i=$(($i+1))
done
cdate=`date +%s`
cp blast_stats logs_weasel_memfs/blast_stats_$cdate
