# data store
- initialize 'tables' with specific fields
## commands
    ./doodle sketch init 
    ./doodle sketch modify
    ...

# file system
- supports hierarchichal file names
- umask / unix permissions
- 8 bit / byte support
- atomic renames
## issues with POSIX compliance
- append only commands
- checkpointing means data may be out-of-date in some replicas
- what does tail / head mean with sketches (they're unordered)?
## hdfs compliance
- we know the number of observations at each node
- along with number of features we know how many blocks at each node
    - does __NOT__ work with dataset filtering
- **VERY** difficult - if not impossible
## commands
    ./doodle dfs append
    ./doodle dfs create
    ./doodle dfs chgrp
    ./doodle dfs chmod
    ./doodle dfs chown
    ./doodle dfs cat
    ./doodle dfs get
    ./doodle dfs head
    ./doodle dfs ls
    ./doodle dfs mkdir
    ./doodle dfs mv
    ./doodle dfs put
    ./doodle dfs stat
    ./doodle dfs tail
