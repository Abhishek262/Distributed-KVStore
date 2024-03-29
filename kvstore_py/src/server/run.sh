#!/bin/bash

RUN_DIR=`dirname "$0"`

python3 kvmaster.py &

function ctrl_c() {
    for kv_pid in $(jobs -p)
    do
        kill "${kv_pid}"
    done
    exit
}
trap ctrl_c INT

until lsof -i :8888 >/dev/null
do
    echo "Waiting for kvmaster to start..."
    sleep 1
done

python3 kvslave.py -t 9001 8888 &
python3 kvslave.py -t 9002 8888 &

RED=`tput setaf 1`
GREEN=`tput setaf 2`
NORMAL=`tput sgr0`

echo "${GREEN}All processes started${NORMAL}"
echo "Now run ${RED}./interactive_client -p 8888${NORMAL} in another terminal to connect"
echo "Or use ${RED}Ctrl+C${NORMAL} to stop all processes"

wait