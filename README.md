# doodle
## OVERVIEW
Distributed sketch-aligned file system presenting HDFS compatible interface.

## TODO
- add control plugins to 'sketch show' command
- integrate yarn resource management (YarnClient)
- hadoop doesn't work well with anamnesis because the block sizes vary between blocks
    - when writing a file to anamensis with hdfs native client hadoop works
