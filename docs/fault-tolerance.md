# description
## replication
- data is periodically checkpointed these checkpoints are replicated
    - a checkpoint is written to disk
    - as sketches are small (ie. order of <10GiB) replication is efficient

## asynchronously checkpoint replication
- synchronous: means during each write we wait for all replicas to be updated
- to allow system usability we don't block during replication
    - our approach send "buffers" of records but handles each individually

## cache consistency
- only one replica is stored in memory
- primary replica is the only machine able to checkpoint a sketch
    - removes possibility for inconsistent caches

# refrences
- https://hal.inria.fr/hal-00789086/file/a_survey_of_dfs.pdf
- http://ijsrcseit.com/paper/CSEIT174443.pdf
- https://sigmodrecord.org/publications/sigmodRecord/1012/pdfs/04.surveys.cattell.pdf
    - almost all NoSQL solutions use asynchronous replication to support fast writes
        - our solution is fast even





# general ideas
- we have a unique issue where data cannot be replicated at the observation granularity, but instead requires entire sketches
- like tachyon / alluxio we don't focus on replication of every step
    - they use a lineage construct to replicate "important" chunks of data
    - instead we periodically checkpoint (manually or automatically) and replicate entire sketches
        - on failure, they recompute intermediate datasets from most recent lineage checkpoint
        - on failure, we re-insert data since last checkpoint
- asynchronous, primary site replication strategy
    - only one copy is able to be updated
    - checkpoints are initilized at the primary site

    - pros
        - ensures fast writes
        - systems may postpone replication traffic under high stress
    - cons
        - data may temporarily differ between replicas
        - data may be lost on host crash


    - con mitigation
        - checkpoints are fast require relatively little disk space (<10GB)
            - frequently checkpoint data

        - may retain transaction log (data loaded) between checkpoints
        - while checkpoints are stored on disk, popular portions of sketch may be memory-resident on any replica



- change propogration
    - minimum spanning tree



# api
./doodle checkpoint list [sketchId]
./doodle checkpoint new <sketchId> <checkpointId>
./doodle checkpoint delete <checkpointId>

# classes
CheckpointManager.java
    Map<String, List<Checkpoint>> checkpoints;

Checkpoint.java
    long timestamp
    String checkpointId
    String sketchId
    Node primary
    Node[] secondaries
