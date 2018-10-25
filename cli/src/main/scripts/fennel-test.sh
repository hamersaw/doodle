#!/bin/bash

# initialize DoodleDhtPlugin
./doodle control init dht com.bushpath.doodle.dht.DoodleDhtPlugin
./doodle control modify dht -a "token:0:-1073741824,1073741824" -a "token:1:0,2147483647"

# initialize DoodleFennelPlugin sketch
./doodle sketch init -c dht noaa-fennel com.bushpath.doodle.fennel.DoodleFennelPlugin
./doodle sketch modify noaa-fennel -a "feature:one:0,5,10" -a "feature:two:0,5,10" -a "feature:three:0,5,10"
./doodle sketch modify noaa-fennel -a "hash:one:-1" -a "hash:two:-1"
