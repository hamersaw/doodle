# doodle
## OVERVIEW
Distributed sketch-aligned file system presenting HDFS compatible interface.

## SKETCH MOCKUP
doodle sketch init noaa-fennel com.bushpath.doodle.fennel.DoodleFennelPlugin
doodle sketch list
doodle sketch modify -a "feature:temperature_surface:0,1,2,3" -a "feature:humidity:0,30,60"
doodle sketch show noaa-fennel
doodle sketch write noaa-fennel noaa1.csv noaa2.csv

### MESSAGES
SKETCH_PROCESS
SKETCH_PROCESSOR_OPEN
SKETCH_PROCESSOR_CLOSE
SKETCH_WRITE

### CLASSES
Distributor
ObservationBuffer
SketchPlugin
    abstract short processorOpen()
    abstract void processorClose(short processId)
    abstract void writeObservation(byte[] bytes)
Processor
    BlockingQueue<ObservatioBuffer>
    abstract void process(float[] observation)

### NOTES
Gossamer
    - sketches are broken apart my time
    - variables to define time interval, and time variable in data
    - use DHT to hash start-time and write all data
        start_timestamp = hash(timestamp - (timestamp % interval))

Fennel
    - dht hashes pre-defined buckets
    - simply use dht based on various features

Synopsis
    - determine geohash based on latitude and longitude fields
    - either use dht hash table or define different GossipPlugin to use geohashes


## TODO
- add control plugins to 'sketch show' command
- implement sketch data insertion
- hadoop doesn't work well with anamnesis because the block sizes vary between blocks
    - when writing a file to anamensis with hdfs native client hadoop works
