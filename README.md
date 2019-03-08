# doodle
## OVERVIEW
doodle is a distributed sketch-aligned database. It also provides a simple extension to present a low-cost, in-memory HDFS emulation interface.

## LOGGING
- __error:__ failures which are unrecoverable
- __warn:__ failures which are recoverable
- __info:__ coarse-grained application progress
- __debug:__ output that may be useful in a debugging setting
- __trace:__ fine-grained output

## COMMANDS
#### COMPILATION
    cd impl
    ./setup.sh
    cd node
    gradle build
    cd ../dfs
    gradle build
    cd ../cli
    gradle build

#### STARTING DOODLE CLUSTER WITH DFS
1. configure hosts in /etc/hosts.txt (format "ip port persistDirectory nodeId")
2. use provided shell script

    ./bin/start-nodes.sh
    ./bin/start-dfs.sh

#### STOPPING DOODLE CLUSTER WITH DFS
    ./bin/stop-nodes.sh
    ./bin/stop-dfs.sh

## TODO
- add control plugins to 'sketch show' command
- **remove NodeMetadata - store Node (protobuf) instead**
#### DFS
- pass secondary replicas as possible block locations
- evict and generate blocks on fly
- implement delete (file and directory) in cli
