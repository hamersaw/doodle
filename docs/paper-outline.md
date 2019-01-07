# introduction
- TODO
## research questions
RQ-1: How can we perform iterative in-memory analytics while mitigating memory contention?
RQ-2: How can we facilitate interopability with diverse distributed analytics platforms?
RQ-3: 

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
- AnalyticsPlane
    - interfaces with HDFS - splittig dataset into blocks
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
    - what is the bottleneck? (initial testing shows csv parsing)

## recovering from failures
- challenge: we have a unique situation where data cannot be replicated at the observation granularity level
    - makes convergence of sketch replicas particlarily difficult after failures
    - unable to identify which records were written to replicas
- solution:
    - periodically write data checkpoints
    - asynchronous, primary site replication
    - manually initiated data rollback
    - restarts read from most recent checkpoint
- pros:
    - fast, efficient writes
- cons:
    - potential to read out-of-date data
- mitigations:
    - sketches are small, checkpointing is fast, perform often
- **figure** -  primary and secondary replica convergence (gantt chart)

## initializing files
- files are initialized with variety of metadata attributes
    - sketch id - unique identifier for sketch
    - query - combination of filters on data
    - data format - data may be generated in a number of formats
- full resolution data is generated impromptu
#### query definition
- query support for numeric constraints (<, <=, ==, >=, >)
    - present SQL-like interface
- on failure, secondary replicas proivde the ability to query on-disk data
    - iteratively parses small portions in-memory (small memory footprint)
- **figure** - compare throughput and latency of cluster queries (on-disk and off-disk)
#### dataset format
- support for myriad file formats
    - __table__ (csv, binary, netcdf, hdf5)

## performing in-memory analytics
- each sketchfs node emulates HDFS namenode and datanode functionality
#### namenode - serving metadata
- file metadata is available on every sketchfs node
    - number of observations at each node
#### datanode - serving blocks
- blocks are computed in view of observation counts at each node
- synthetic datasets are generated impromptu
    - alleviates memory contention
- sketch specific algorithms are defining within plugins
    - fennel - TODO - describe
    - synopsis - TODO - describe
- **figure** - speed of generating synthetic datasets
- **figure** - synthetic dataset statistical representation (kruskal-wallis, and interfeature)

# evaluation
## evaluation setup
- datasets: imputed NOAA 18TB
- machines: ...
- software: ...
## memory usage
- **figure** - compare how much data can be stored in-memory by each system
    - MongoDB, Cassandra, HDFS, HyperDex, DB-X
## system peformance
- **figure** - use YCSB (Yahoo! Service Cloud Benchmark) to compare throughput of systems
    - only shortcoming - we really only support range queries instead of individual records

# related work
## lossy compression algorithms
- isabella
- numarck
- sz
## hdfs compliant variants
- tachyon / aluxio
- triple-h
## 
