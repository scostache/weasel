#!/bin/bash

# $1 is the input directory

export PATH=$PATH:/home/$USER/Montage_v3.3/bin:/usr/local/bin:/usr/bin:/bin:/usr/local/games:/usr/games:/home/$USER/Montage_v3.3/bin:/tmp/Weasel/bin/

for file in `ls $1/statdir/`; do   
    echo $1/statdir/${file} ;   
    cat $1/statdir/${file} ; 
done > $1/stats.tmp

/home/$USER/Weasel/scripts/examples/Montage_DAS4/format-fits.py $1/stats.tmp $1/fits.tbl

mImgtbl $1/projdir $1/images.tbl

mBgModel $1/images.tbl $1/fits.tbl $1/corrections.tbl
