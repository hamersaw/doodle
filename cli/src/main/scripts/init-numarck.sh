#!/bin/bash

# initialize DoodleNumarckPlugin sketch
./doodle sketch init sketch_one com.bushpath.doodle.numarck.DoodleNumarckPlugin dht

# add features to sketch
./doodle sketch modify sketch_one \
    -a "feature:temperature_surface:-.4,-.35,-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3,.35,.4" \
    -a "feature:relative_humidity_zerodegc_isotherm:-.4,-.35,-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3,.35,.4" \
    -a "feature:surface_wind_gust_surface:-.4,-.35,-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3,.35,.4" \
    -a "feature:pressure_surface:-.4,-.35,-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3,.35,.4" 
