#!/bin/bash

# initialize DoodleIsabellaPlugin sketch
./doodle sketch init sketch_one com.bushpath.doodle.isabella.DoodleIsabellaPlugin dht
./doodle sketch modify sketch_one -a "feature:temperature_surface:-1", -a "feature:relative_humidity_zerodegc_isotherm:-1" -a "feature:surface_wind_gust_surface:-1" -a "feature:pressure_surface:-1" -a "knotCount:-1:6"
