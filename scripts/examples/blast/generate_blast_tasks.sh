#!/bin/bash

dir=$1

mypath=`pwd`

# dir is the input directory

source $HOME/.bashrc

QUERY_FILE=/tmp/memfs2/query_big.fastaa

cd $dir
export INITIAL_FILES=`ls -l frag* | tr -s ' ' | cut -d ' ' -f 9 | head -n 300`
cd $mypath

rm tasks.txt

j=2

rm tmp_tasks

for file in $INITIAL_FILES ;
do
    if [ $j -eq 5 ]; then
        j=2
    fi
    outdir=/tmp/memfs$j/
    exec_name="/home/$USER/blast-2.2.26/bin/formatdb"
    params="-i /tmp/memfs$j/${file} -p F -o T > /tmp/memfs$j/${file}.out"
    inputs="/tmp/memfs$j/${file}"
    outputs="/tmp/memfs$j/${file}.out"
    echo "${file} $outdir" >> tmp_tasks
    echo "exec=$exec_name" >> tasks.txt
    echo "params=$params" >> tasks.txt
    echo "inputs=$inputs" >> tasks.txt
    echo "outputs=$outputs" >> tasks.txt
    j=$(($j+1))
done

j=2
for file in $INITIAL_FILES ;
do
    if [ $j -eq 5 ]; then
        j=2
    fi
    outdir=/tmp/memfs$j/
    for i in {1..3} ; do # 6, initialy there were 16 queries
        echo "blastall -p blastn -i ${QUERY_FILE} -d /tmp/memfs$j/$file -o /tmp/memfs$j/${file}_${i}_output.txt" >> job_file_2.txt
        j=$(($j+1))
   	exec_name="/home/$USER/blast-2.2.26/bin/blastall"
	params="-p blastn -i ${QUERY_FILE} -d /tmp/memfs$j/$file -o /tmp/memfs$j/${file}_${i}_output.txt"
	dir1=`grep $file tmp_tasks | cut -d ' ' -f 2`
	inputs="${QUERY_FILE} ${dir1}$file ${dir1}$file.out" 
	outputs="${dir1}${file}_${i}_output.txt"
	echo "exec=$exec_name" >> tasks.txt
	echo "params=$params" >> tasks.txt
	echo "inputs=$inputs" >> tasks.txt
	echo "outputs=$outputs" >> tasks.txt
    done
done

echo "Done generating tasks"
