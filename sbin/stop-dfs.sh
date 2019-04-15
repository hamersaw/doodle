#!/bin/bash

# check arguments
if [ $# != 0 ]
then
    echo "usage: $0"
    exit 1
fi

# intialize instance variables
DOODLEDIR="$(pwd)/$(dirname $0)/.."
HOSTS_PATH="$DOODLEDIR/etc/hosts.txt"

# iterate over hosts file
while read LINE; do
    # parse line into array
    ARRAY=($LINE)

    echo "stopping node ${ARRAY[3]} - ${ARRAY[0]}:${ARRAY[1]}"

    if [ "${ARRAY[0]}" == "127.0.0.1" ]
    then
        # if local no 'ssh' necessary
        PID=$(cat "$DOODLEDIR/log/dfs-node-${ARRAY[3]}.pid")
        kill -9 $PID
        rm "$DOODLEDIR/log/dfs-node-${ARRAY[3]}.pid"
    else
        # remotely stop node
        ssh rammerd@${ARRAY[0]} -n "kill `cat $DOODLEDIR/log/dfs-node-${ARRAY[3]}.pid`; \
            rm $DOODLEDIR/log/dfs-node-${ARRAY[3]}.pid"
    fi
done < $HOSTS_PATH
