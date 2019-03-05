#!/bin/bash

# initialize DoodleDhtPlugin
./doodle control init dht com.bushpath.doodle.dht.DhtPlugin
./doodle control modify dht \
    -a "token:0:-2147483648,-715827883,715827882"
