#!/bin/bash

# initialize DoodleDhtPlugin
./doodle control init dht com.bushpath.doodle.dht.DhtPlugin
./doodle control modify dht -a "token:0:0,2147483646" -a "token:1:-536870911,1610612733" -a "token:2:-1073741822,1073741822" -a "token:3:-1610612733,536870911"
