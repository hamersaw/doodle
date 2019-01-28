#!/bin/sh
APPLICATION="doodle-node"
VERSION="0.1-SNAPSHOT"
MAIN_CLASS="com.bushpath.doodle.node.Main"

CLASSPATH=""
BASEDIR=$(dirname $0)
HOSTNAME=$(hostname)
if [ -f $BASEDIR/build/libs/$APPLICATION-$VERSION.jar ]; then
    CLASSPATH="$BASEDIR/build/libs/$APPLICATION-$VERSION.jar"
else
    echo "unable to find $APPLICATION-$VERSION.jar."
    exit 1
fi

JAVA_OPTS="-Xmx12G -Dorg.slf4j.simpleLogger.defaultLogLevel=info --illegal-access=deny -Djava.library.path=$BASEDIR/libs/anamnesis-jni/"

java -cp $CLASSPATH $JAVA_OPTS $MAIN_CLASS $@ > $BASEDIR/$HOSTNAME.log 2>&1 &
echo $! > $BASEDIR/$HOSTNAME.pid
