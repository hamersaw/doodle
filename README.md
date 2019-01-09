# doodle
## OVERVIEW
Distributed sketch-aligned file system presenting HDFS compatible interface.

## LOGGING
- __error:__ failures which are unrecoverable
- __warn:__ failures which are recoverable
- __info:__ coarse-grained application progress
- __debug:__ output that may be useful in a debugging setting
- __trace:__ fine-grained output

## TODO
- optimize sketch observation counts without generating data
- on file creation compute observation counts on checkpoints at each node
- add control plugins to 'sketch show' command
- integrate yarn resource management (YarnClient)
