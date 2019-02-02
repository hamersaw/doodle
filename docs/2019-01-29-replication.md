# replication
- maintain journal of data at primary replica
    - journal is transformed data
    - tracks which replicas have gathered data
        - once all replicas retrieved data -> delete it

- each sketch replica writes data to disk
    - maintains an index of where data is
        Synopsis - forest of trees - leaf nodes are offsets
        Fennel - hashmap - values are offsets
        ISABELLA - treemap - values are file offsets
        NUMARCK - treempa - values are file offsets
    - as offsets are maintained - data can be cached as well

# process
1. data buffered and round-robin sent to SketchFS nodes by client
    - # of SketchFS nodes configurable (for scalability)
    - buffer sizes configurable (algorithms may benefit from difference)
2. observations are transformed and shuffeled
    - shuffeling based on data co-locality definitions
3. journal writes transformed data
    - using lsm tree / ss tables?
4. asynchronously each replica consults journal at primary replica
    - eventual consistency
5. replicas write data to disk
    - maintaining index over data to query efficiently
    - all replicas may temporarily cache hot data
6. once all replicas update, journal deletes data

# classes
## new
JournalService.java
    write();
    diff();

## modifications
SketchPlugin.java
    + short replicaType; (PRIMARY, SECONDARY, etc)
    + long journalTimestamp;
    + List<Replicas> replicas;

    + each plugin writes data to disk
SketchService.java
    + JournalUpdateTimerTask.java

Plugin.java
    + needs to write everything to disk - and reload of restart
