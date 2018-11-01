#!/bin/bash

# initialize DoodleDhtPlugin
./doodle control init dht com.bushpath.doodle.dht.DoodleDhtPlugin
./doodle control modify dht -a "token:0:-1073741824,1073741824" -a "token:1:0,2147483647"

# initialize DoodleFennelPlugin sketch
./doodle sketch init -c dht sketch_one com.bushpath.doodle.fennel.DoodleFennelPlugin
./doodle sketch modify sketch_one -a "hash:temperature_surface:-1" -a "hash:pressure_surface:-1"

# initialize DoodleSynopsisPlugin sketch
#./doodle sketch init -c dht sketch_one com.bushpath.doodle.synopsis.DoodleSynopsisPlugin
#./doodle sketch modify sketch_one -a "latitudeFeature:latitude:-1" -a "longitudeFeature:longitude:-1" -a "geohashPrecision:-1:4"

# add features to sketch
./doodle sketch modify sketch_one \
    -a "feature:temperature_surface:232.42301635742223,268.6041542884687,273.9606715298498,280.56594739192093,283.6549818746806,286.2839473919228,288.5842922195098,291.0489473919244,293.18498187468373,295.41960256433964,297.06270601261605,298.70580946089245,300.44749911606544,302.0248784264108,327.7230163574538" \
    -a "feature:relative_humidity_zerodegc_isotherm:0.0,4.213433333333016,7.830033333332946,13.764966666666167,23.40923333333293,33.099866666666365,43.57873333333243,53.22299999999854,62.58906666666467,70.51776666666521,76.59179999999913,82.06306666666632,86.88520000000011,92.3564666666673,96.29763333333435,100.0" \
    -a "feature:surface_wind_gust_surface:0.0,1.0617455382347378,1.4893705382347378,1.8392455382347377,2.1988392882347423,2.6070267882347515,3.0346517882347612,3.4039642882347696,3.724683038234777,4.025964288234784,4.3272455382347905,4.638245538234798,4.949245538234805,5.289401788234812,5.687870538234821,6.05718303823483,6.377901788234837,6.698620538234844,7.038776788234852,7.38865178823486,7.738526788234868,8.117558038234865,8.574339288234833,9.0505580382348,9.555933038234766,10.265401788234717,10.897120538234674,11.519120538234631,12.41324553823457,13.268495538234511,14.648558038234416,17.20458928823446,28.040995538235695" \
    -a "feature:pressure_surface:64264.90000000174,83499.7141760733,88166.7580013333,92844.10435159388,95090.054801719,96573.61840180165,97542.0557518556,98562.00572691242,99684.98095197498,100447.36780201746,100838.86375203928,101057.7924083015,101225.20843956084,101369.44378956889,101503.37661457637,101637.30943958384,101778.96915834174,101943.80955835094,102165.31384586329,102577.41484588625,105482.7268960481"
