#!/bin/bash

# initialize DoodleDhtPlugin
./doodle control init dht com.bushpath.doodle.dht.DoodleDhtPlugin
./doodle control modify dht -a "token:0:0,2147483646" -a "token:1:-536870911,1610612733" -a "token:2:-1073741822,1073741822" -a "token:3:-1610612733,536870911"

# initialize DoodleIsabellaPlugin sketch
./doodle sketch init -c dht sketch_one com.bushpath.doodle.isabella.DoodleIsabellaPlugin
./doodle sketch modify sketch_one -a "feature:temperature_surface:-1", -a "feature:relative_humidity_zerodegc_isotherm:-1" -a "feature:surface_wind_gust_surface:-1" -a "feature:pressure_surface:-1" -a "knotCount:-1:6"
