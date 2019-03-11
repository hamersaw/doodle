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
CONFIG_PATH="$DOODLEDIR/etc/node-config.toml"

APPLICATION="doodle-node"
VERSION="0.1-SNAPSHOT"
MAIN_CLASS="com.bushpath.doodle.node.Main"

CLASSPATH=""
if [ -f $DOODLEDIR/impl/node/build/libs/$APPLICATION-$VERSION.jar ]; then
    CLASSPATH="$DOODLEDIR/impl/node/build/libs/$APPLICATION-$VERSION.jar"
else
    echo "unable to find $APPLICATION-$VERSION.jar."
    exit 1
fi

JAVA_OPTS="-Xmx2G -Dorg.slf4j.simpleLogger.defaultLogLevel=info --illegal-access=deny"

# iterate over hosts file
while read LINE; do
    # parse line into array
    ARRAY=($LINE)

    echo "starting node ${ARRAY[3]} - ${ARRAY[0]}:${ARRAY[1]}"

    if [ "${ARRAY[0]}" == "127.0.0.1" ]
    then
        # if local no 'ssh' necessary
        java -cp $CLASSPATH $JAVA_OPTS $MAIN_CLASS $@ \
            ${ARRAY[0]} ${ARRAY[1]} ${ARRAY[2]} \
            ${ARRAY[3]} $HOSTS_PATH $CONFIG_PATH \
                > $DOODLEDIR/log/node-${ARRAY[3]}.log 2>&1 &

        echo $! > $DOODLEDIR/log/node-${ARRAY[3]}.pid
    else
        # remotely start node
        ssh rammerd@${ARRAY[0]} -n "java -cp $CLASSPATH $JAVA_OPTS $MAIN_CLASS $@ \
            ${ARRAY[0]} ${ARRAY[1]} ${ARRAY[2]} \
            ${ARRAY[3]} $HOSTS_PATH $CONFIG_PATH \
                > $DOODLEDIR/log/node-${ARRAY[3]}.log 2>&1 & \
            echo \$! > $DOODLEDIR/log/node-${ARRAY[3]}.pid"
    fi
done < $HOSTS_PATH
