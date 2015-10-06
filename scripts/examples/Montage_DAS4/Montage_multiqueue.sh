#!/bin/bash

dir=$1
data_dir=$2

source $HOME/.bashrc

mkdir $1/projdir $1/diffdir $1/statdir $1/corrdir $1/subsets $1/regions $1/partial_corrections $1/final $1/rawdir

export PATH=${PATH}:/home/costache/Montage_v3.3/bin:/local/costache/Weasel/bin/
export PYTHONPATH=$PYTHONPATH:/home/costache/pyzmq-14.0.1/build/lib.linux-x86_64-2.6/:/local/costache/Weasel/

echo "Starting COPY"

start_time=`date +%s`

echo $start_time >> montage_stats_multi

cp $data_dir/newregion.hdr $1/newregion.hdr
cp $data_dir/diffs.tbl $1/
cp -r $data_dir/rawdir $1/
end_time=`date +%s`

echo "Data copy time was `expr $end_time - $start_time` s." >> montage_stats_multi

echo "Sending tasks to execution"

start_time=`date +%s`

app_id=`client queue -f ./tasks.txt | grep "Application ID" | cut -d ':' -f 2`  

echo $app_id

sleep 10
client waitall -i $app_id

end_time=`date +%s`

echo "Execution time was `expr $end_time - $start_time` s." >> montage_stats_multi
