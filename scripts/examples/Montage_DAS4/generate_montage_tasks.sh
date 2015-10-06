#!/bin/bash

dir=$1

outdir=$2

# dir is the input directory

source $HOME/.bashrc

echo "Generating image tables"

#mImgtbl $dir/rawdir $dir/images-rawdir.tbl
#mMakeHdr $dir/images-rawdir.tbl $dir/newregion.hdr

#mDAGTbls $dir/images.tbl $dir/new_region.hdr $dir/new_images.tbl $dir/proj_images.tbl $dir/corr_images.tb

#mOverlaps $dir/proj_images.tbl $dir/diffs.tbl

echo "Generating mProjectPP"

start_time=`date +%s`

rm tasks.txt

j=2

rm tmp_tasks
for file in `ls $1/rawdir`
do
    if [ $j -eq 5 ]; then
	j=2
    fi
    outdir=/local/$USER/memfs$j/
    exec_name="/home/$USER/Montage_v3.3/bin/mProjectPP"
    params="${outdir}rawdir/${file} ${outdir}projdir/p${file} ${outdir}newregion.hdr"
    inputs="${outdir}rawdir/${file} ${outdir}newregion.hdr"
    NAME=`echo "${file}" | cut -d'.' -f1`
    EXTENSION=`echo "${file}" | cut -d'.' -f2`
    echo "p$file $outdir" >> tmp_tasks
    outputs="${outdir}projdir/p${file} ${outdir}projdir/p${NAME}_area.$EXTENSION"
    echo "exec=$exec_name" >> tasks.txt
    echo "params=$params" >> tasks.txt
    echo "inputs=$inputs" >> tasks.txt
    echo "outputs=$outputs" >> tasks.txt
    j=$(($j+1))
done 

echo "Generating mDiffFIt"

j=2
i=0
while read line
do

    i=$(($i+1))

    if [ $i -lt 3 ]; then
	continue
    fi
    if [ $j -eq 5 ]; then
        j=2
    fi
    outdir=/local/$USER/memfs$j/
    exec_name="export PATH=/home/$USER/Montage_v3.3/bin; /home/$USER/Montage_v3.3/bin/mDiffFit"
    params=`echo $line | awk -v p=$outdir '{printf("%s/projdir/%s %s/projdir/%s %s/diffdir/diff.%0.6d.%0.6d.fits \
	    %s/newregion.hdr > %s/statdir/stats-diff.%0.6d.%0.6d.fits", p, $3, p,$4, p,$1, $2, p, p, $1, $2)}'`
    input1=`echo $line | awk '{printf($3)}'`
    input2=`echo $line | awk '{printf($4)}'`
    dir1=`grep $input1 tmp_tasks | cut -d ' ' -f 2`
    dir2=`grep $input2 tmp_tasks | cut -d ' ' -f 2`
    output=`echo $line | awk '{printf($5)}'`
    EXTENSION=`echo "${input1}" | cut -d'.' -f2`
    name_input1=`echo $input1 | cut -d '.' -f1`
    name_input2=`echo $input2 | cut -d '.' -f1`
    inputs="${dir1}projdir/$input1 ${dir1}projdir/${name_input1}_area.${EXTENSION} \
		${dir2}projdir/$input2 ${dir2}projdir/${name_input2}_area.${EXTENSION}"

    echo "exec=$exec_name" >> tasks.txt
    echo "params=$params" >> tasks.txt 
    echo "inputs=$inputs" >> tasks.txt
    echo "outputs=${outdir}diffdir/$output ${outdir}statdir/stats-$output" >> tasks.txt
    j=$(($j+1))
done < $dir/diffs.tbl


echo "Generating mBgModel"

exec_name="/home/$USER/Weasel/scripts/examples/Montage_DAS4/run_bgmodel.sh"
echo "exec=$exec_name" >> tasks.txt
params="$outdir"
echo "params=$params" >> tasks.txt
input=""

j=2
i=0
while read line; do
    i=$(($i+1))
    if [ $i -lt 3 ]; then
        continue
    fi
    if [ $j -eq 5 ]; then
        j=2
    fi
    outdir=/local/$USER/memfs$j/
    file_name=`echo $line | awk -v p=$outdir '{printf("%sstatdir/stats-diff.%0.6d.%0.6d.fits"\
	,p,$1,$2)}'`
    input="$input $file_name"
    j=$(($j+1))
done < $dir/diffs.tbl

outdir=/local/$USER/memfs$j/

echo "inputs=$input" >> tasks.txt
output="${outdir}corrections.tbl"
echo "outputs=$output" >> tasks.txt

echo "Generating mBackground"

python /home/$USER/Weasel/scripts/examples/Montage_DAS4/gen-back-tasks-2.py $dir/images.tbl $dir/corrections.tbl >> ./tasks.txt

echo "Done generating tasks"
