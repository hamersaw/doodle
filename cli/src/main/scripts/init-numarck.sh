#!/bin/bash

# initialize DoodleDhtPlugin
./doodle control init dht com.bushpath.doodle.dht.DoodleDhtPlugin
./doodle control modify dht -a "token:0:0,2147483646" -a "token:1:-536870911,1610612733" -a "token:2:-1073741822,1073741822" -a "token:3:-1610612733,536870911"

# initialize DoodleNumarckPlugin sketch
./doodle sketch init -c dht sketch_one com.bushpath.doodle.numarck.DoodleNumarckPlugin

# add features to sketch
./doodle sketch modify sketch_one \
    -a "feature:temperature_surface:-.4,-.35,-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3,.35,.4" \
    -a "feature:relative_humidity_zerodegc_isotherm:-.4,-.35,-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3,.35,.4" \
    -a "feature:surface_wind_gust_surface:-.4,-.35,-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3,.35,.4" \
    -a "feature:pressure_surface:-.4,-.35,-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3,.35,.4" 
