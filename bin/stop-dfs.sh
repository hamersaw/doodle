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

    if [ "${ARRAY[0]}" == "127.0.0.1" ]
    then
        # if local no 'ssh' necessary
        echo "stopping local node ${ARRAY[3]} - ${ARRAY[0]}:${ARRAY[1]}"

        PID=$(cat "$DOODLEDIR/log/dfs-node-${ARRAY[3]}.pid")
        kill -9 $PID
        rm "$DOODLEDIR/log/dfs-node-${ARRAY[3]}.pid"
    else
        # remotely start node
        echo "TODO - start remote node"
    fi
done < $HOSTS_PATH
