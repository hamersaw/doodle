# systems overview
## data sketching
#### fennel
- discretized feature vectors (DFVs)
- distributed hash table (DHT)
- **figure**
#### synopsis
- welfords method
- forest of trees
- **figure**
## node architecture
- ControlPlane
    - responsbile for determining where data resides in cluster
- SketchPlane
    - responsible for storage/retrieval of data
- **figure** - show ControlPlane vs SketchPlane


# methodology

## sketching data within the system
#### plugin architecture
- extensible plugin architecture (java jar file)
- ControlPlugin
    - passed to SketchPlugin on intialization to consult during observation distribution
    - utilized by clients to determine query optimizations
    - implemented simple DHT in ~200 LoC
- SketchPlugin
    - __table__ - required functionality abstraction (10 abstract methods)
        - add / remove variables
        - transformation and distribution of observations
        - add observation to sketch
        - data query handling
        - synthetic dataset generation
        - getting sketch features
        - computing observation counts
        - closing various data structures
    - implemented fennel and synopsis algorithms in ~500 LoC each
#### scaling data insertion
- push computationally expensive operations to nodes
    - eg. geohash computation (synopsis), observation binning (fennel)
- client is only responsible for parsing raw data and sending buffers (float[])
- nodes consult ControlPlugin's to route buffered observations to responsible nodes
    - format is defined between sketch algorithms (ex. fennel (byte[]), synopsis (float[]))
- SketchPlugin handles raw data writes
- **figure** - how fast can we insert data (observations / second)
    - insert into cluster with increasing number of nodes
    - what is the bottleneck? csv parsing?

## recovering from failures
- challenge: we have a unique situation where data cannot be replicated at the observation granularity level
    - makes convergence of sketch replicas particlarily difficult after failures
    - unable to identify which records were written to replicas
- solution:
    - periodically write data checkpoints
    - asynchronous, primary site replication
    - journaling filesystem
    - manually initiated data rollback
- pros:
    - fast, efficient writes
- cons:
    - potential to read out-of-date data
- mitigations:
    - sketches are small, checkpointing is fast, perform often
- **figure** -  primary and secondary replica convergence (gantt chart)

## querying and retrieving sketched data
- query support for numeric constraints (<, <=, ==, >=, >)
    - present SQL-like interface
- data generated client-side
    - algorithm defined in SketchPlugin code
- on failure, secondary replicas proivde the ability to query on-disk data
    - iteratively parses small portions in-memory (small memory footprint)
- **figure** - querying on in-memory vs on-disk (failure)

## generation of synthetic datasets
- data in transit is sketched
- generate synthetic observations client-side (very fast)
- sketch specific algorithms defined during plugin implementation
    - fennel - 
    - synopsis - 
- **figure** - speed of querying vs. generating synthetic datasets
- **figure** - synthetic dataset statistical representation (kruskal-wallis, and interfeature)

## analytics
- by using Anamnesis we can present an in-memory, sketch aligned, HDFS compatible interface
- reference paper to show speed improvements over HDFS


# evaluation
## evaluation setup
- datasets: imputed NOAA 18TB
- machines: ...
- software: ...
## memory usage
- **figure** - compare how much data can be stored in-memory by each system
    - MongoDB, Cassandra, HDFS, HyperDex, DB-X
## throughput
- **figure** - use YCSB (Yahoo! Service Cloud Benchmark) to compare throughput of systems
    - only shortcoming - we really only support range queries instead of individual records
## throughput vs. latency
- **figure** - compare throughput and latency of cluster queries
