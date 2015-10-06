#!/bin/bash
export DIR=/local/$USER/
export TILES=4
mkdir $DIR/regions
mkdir $DIR/subsets
mkdir $DIR/partial_corrections
mkdir $DIR/shrinks

echo "started mTileHdr"
for ((i=0;$i<$TILES;i++)) ; do
        for ((j=0;$j<$TILES;j++)) ; do
		exec_name="mTileHdr"
		params="$DIR/newregion.hdr $DIR/regions/region_${i}_${j}.hdr $TILES $TILES $i $j"
		inputs=$DIR/newregion.hdr
		outputs=$DIR/regions/region_${i}_${j}.hdr
		echo "exec=$exec_name" >> tasks2.txt
    		echo "params=$params" >> tasks2.txt
    		echo "inputs=$inputs" >> tasks2.txt
		echo "outputs=$outputs" >> tasks2.txt	
                #mTileHdr $DIR/newregion.hdr $DIR/regions/region_${i}_${j}.hdr $TILES $TILES $i $j
	done
done
echo "done mTileHdr"

echo "started mSubset"
for ((i=0;$i<$TILES;i++)) ; do
        for ((j=0;$j<$TILES;j++)) ; do
		exec_name="mSubset"
                params="$DIR/corrections.tbl $DIR/regions/region_${i}_${j}.hdr $DIR/subsets/subset_${i}_${j}.tbl"
                inputs=$DIR/corrections.tbl $DIR/regions/region_${i}_${j}.hdr
                outputs=$DIR/subsets/subset_${i}_${j}.tbl
		echo "exec=$exec_name" >> tasks2.txt
                echo "params=$params" >> tasks2.txt
                echo "inputs=$inputs" >> tasks2.txt
                echo "outputs=$outputs" >> tasks2.txt
                #mSubset $DIR/corr-images.tbl $DIR/regions/region_${i}_${j}.hdr $DIR/subsets/subset_${i}_${j}.tbl &
        done
done
wait
echo "done mSubset"

echo "started parallel mAdd"
for ((i=0;$i<$TILES;i++)) ; do
        for ((j=0;$j<$TILES;j++)) ; do
		exec_name="mAdd"
                params="-n -p $DIR/corrdir $DIR/subsets/subset_${i}_${j}.tbl $DIR/regions/region_${i}_${j}.hdr $DIR/partial_corrections/corr-img_${i}_${j}.fits"
                inputs=$DIR/corrdir $DIR/subsets/subset_${i}_${j}.tbl $DIR/regions/region_${i}_${j}.hdr
                outputs=$DIR/partial_corrections/corr-img_${i}_${j}.fits
		echo "exec=$exec_name" >> tasks2.txt
                echo "params=$params" >> tasks2.txt
                echo "inputs=$inputs" >> tasks2.txt
                echo "outputs=$outputs" >> tasks2.txt
                #mAdd -n -p $DIR/corrdir $DIR/subsets/subset_${i}_${j}.tbl $DIR/regions/region_${i}_${j}.hdr $DIR/partial_corrections/corr-img_${i}_${j}.fits &
        done
done
wait
echo "done parallel mAdd"

echo "started parallel mShrink"
for ((i=0;$i<$TILES;i++)) ; do
        for ((j=0;$j<$TILES;j++)) ; do
		exec_name="mShrink"
		inputs=$DIR/partial_corrections/corr-img_${i}_${j}.fits
                params=$DIR/partial_corrections/corr-img_${i}_${j}.fits $DIR/shrinks/shrunken_${i}_${j}.fits 4
		outputs=$DIR/shrinks/shrunken_${i}_${j}.fits
                echo "exec=$exec_name" >> tasks2.txt
                echo "params=$params" >> tasks2.txt
                echo "inputs=$inputs" >> tasks2.txt
                echo "outputs=$outputs" >> tasks2.txt
        done
done
wait
echo "done parallel mShrink"

time rm /local/$USER/corrdir/*

exec_name="mImgtbl"
inputs=""
for ((i=0;$i<$TILES;i++)) ; do
        for ((j=0;$j<$TILES;j++)) ; do
	inputs="$inputs $DIR/shrinks/shrunken_${i}_${j}.fits"
done
done
outputs=$DIR/shrinks.tbl
params=$inputs $DIR/shrinks.tbl
echo "exec=$exec_name" >> tasks2.txt
echo "params=$params" >> tasks2.txt
echo "inputs=$inputs" >> tasks2.txt
echo "outputs=$outputs" >> tasks2.txt

exec_name="mMakeHdr"
outputs=$DIR/shrinks.hdr
params=$DIR/shrinks $DIR/shrinks.hdr
echo "exec=$exec_name" >> tasks2.txt
echo "params=$params" >> tasks2.txt
echo "inputs=$inputs" >> tasks2.txt
echo "outputs=$outputs" >> tasks2.txt


echo "started final mAdd"
exec_name="mAdd"
inputs="$inputs $DIR/shrinks.tbl $DIR/shrinks.hdr"
params=-n -p $DIR/shrinks $DIR/shrinks.tbl $DIR/shrinks.hdr $DIR/final_shrink.fits
outputs=$DIR/final_shrink.fits
echo "exec=$exec_name" >> tasks2.txt
echo "params=$params" >> tasks2.txt
echo "inputs=$inputs" >> tasks2.txt
echo "outputs=$outputs" >> tasks2.txt
echo "done final mAdd"

echo "started generating final JPEG image"
exec_name="mJPEG"
inputs=$DIR/final_shrink.fits
params=-gray $DIR/final_shrink.fits 0s max gaussian-log -out $DIR/final_pic.jpg
outputs=$DIR/final_pic.jpg
echo "exec=$exec_name" >> tasks2.txt
echo "params=$params" >> tasks2.txt
echo "inputs=$inputs" >> tasks2.txt
echo "outputs=$outputs" >> tasks2.txt
echo "done generating final JPEG image"
