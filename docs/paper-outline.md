# systems overview
## data sketching
- data sketching algorithms provide lossy compression
- present fennel and synopsis in table? (distribution, data structure, representation)
#### fennel
- DFV (discretized feature vector) generation
- distribution based on hash of subset of features
- figure - fennel storage architecture
#### synopsis
- forest of trees
- welfords method
- figure - synopsis storage architecture

## sketchfs node architecture
- ControlPlane: determines data placement
- SketchPlane: handle storage and retrieval of observations
- figure - node


# methodology
## extensible functionality
- ControlPlugin's determine data placement within cluster
- SketchPlugin's handle storage / retrieval
- java jar extending Control/Sketch Plugin class
### control plugin
- passed to SketchPlugin on intialization to consult during observation distribution
- utilized by clients to determine query optimizations
- implemented simple DHT in ~200 LoC
### sketch plugin
- required functionality abstraction (10 abstract methods)
    - add / remove variables
    - transformation and distribution of observations
    - add observation to sketch
    - data query handling
    - synthetic dataset generation
    - getting sketch features
    - computing observation counts
    - closing various data structures
- implemented fennel and synopsis in ~500 LoC each
### plugin configuration
- add / remove variables
    - each SketchPlugin determines how to use them
- examples: feature bin boundaries (fennel), geohash precision (synopsis), node tokens (dht)

## data operations
#### insertion
- support for diverse file formats
- client simply sends observation buffer to cluster nodes
- push computation expensive operations to a subset of cluster nodes
    - eg. geohash computation (synopsis), observation binning (fennel)
#### querying
- simple range-based query format
- support for SQL-like interface
    - range constraints
    - sketching algorithms independently handle query evaluation 
        - relax boundaries
#### generation of synthetic datasets
- present in diverse file formats
- client side inflation
- observation count multiplier
- note on statistical representativeness of both fennel and synopsis sketches

## fault-tolerance
- challenge: we have a unique situation where data cannot be replicated at observation level (granulatity of storage)
    - makes convergence of sketch replicas particlarily difficult after failures
    - unable to identify which records were written to replicas
- solution: maintain periodic checkpoints of data
#### checkpointing algorithm
- periodically checkpoint data (write sketch to disk)
- asynchronous, primary site replication
    - only primary copy may be written to
    - secondary replicas are asychronously updated
    - pros: fast writes, postpone replication under high stress
    - cons: potential for reading "out of data" data, loss of data on crash
    - mitigations: checkpointing is cheap (small data structures, fast), rollback
- figure - secondary replica convergence duration

## efficient resource utilization
- sketching reduces dataset sizes
#### network and disk i/o
- network i/o is reduced as all data in transit is sketched
- checkpointing and failure recovery are only operations requiring disk i/o
#### memory-footprint
- able to query on-disk data for secondary replicas
- integration with YARN

## analytics
#### hdfs compatability
- anamnesis / tachyon / ... ?


# evaluation
- compare to Cassandra, MongoDB, HyperDex, DB-X, HDFS
## memory footprint
- use succinct style where we compare the amount of data stored in-memory against MongoDB, Cassandra, HDFS, etc
    - support different sketches
## throughput
- use Yahoo! Cloud Serving Benchmark tool to profile different sketches
## analytics
- profile dataset filtering against HDFS and analytics with Spark
