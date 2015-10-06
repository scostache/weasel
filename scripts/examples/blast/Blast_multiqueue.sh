#!/bin/sh

input=$1
dir=$2

export PATH=${PATH}:/home/$USER/blast-2.2.26/bin:/local/$USER/Weasel/bin/
export PYTHONPATH=$PYTHONPATH:/local/$USER/Weasel/:/home/$USER/pyzmq-14.0.1/build/lib.linux-x86_64-2.6/

echo "Copying input to in-memory file system"

cp /home/$USER/Weasel/scripts/examples/blast/query_big.fastaa $dir/
cp $input/* $dir/

echo "Submitting tasks...."

start_time=`date +%s`

app_id=`client queue -f ./tasks.txt | grep "Application ID" | cut -d ':' -f 2`

echo $app_id
echo "waiting for all tasks to finish!"
client waitall -i $app_id

end_time=`date +%s`

echo "blast execution time was `expr $end_time - $start_time` s." >> blast_stats_multi
