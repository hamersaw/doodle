# doodle
## OVERVIEW
Distributed sketch-aligned file system presenting HDFS compatible interface.

## LOGGING
- error: failures which are unrecoverable
- warn: failures which are recoverable
- info: coarse-grained application progress
    - adding / removing checkpoints
    - adding / removing sketches
    - adding / removing variables
- debug: output that may be useful in a debugging setting
    - variable values
- trace: fine-grained output
    - message handling

## TODO
- add control plugins to 'sketch show' command
- integrate yarn resource management (YarnClient)
