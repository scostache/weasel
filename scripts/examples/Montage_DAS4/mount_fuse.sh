#! /bin/bash

mkdir $1
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/$USER/lib ; nohup $HOME/memFSredis/memcachefs $1 -o max_read=131072 -o max_write=131072  -o direct_io 2>&1 >$HOME/errorlog.txt &
