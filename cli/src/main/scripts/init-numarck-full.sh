#!/bin/bash

# initialize DoodleNumarckPlugin sketch
./doodle sketch init sketch_one com.bushpath.doodle.numarck.NumarckPlugin dht

# add features to sketch
./doodle sketch modify sketch_one \
	-a "feature:albedo_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:categorical_freezing_rain_yes1_no0_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:categorical_ice_pellets_yes1_no0_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:categorical_rain_yes1_no0_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:categorical_snow_yes1_no0_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:convective_available_potential_energy_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:convective_inhibition_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:direct_evaporation_cease_soil_moisture_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:downward_long_wave_rad_flux_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:downward_short_wave_rad_flux_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:drag_coefficient_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:friction_velocity_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:geopotential_height_lltw:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:geopotential_height_pblri:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:geopotential_height_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:geopotential_height_zerodegc_isotherm:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:ice_cover_ice1_no_ice0_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:land_cover_land1_sea0_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:latent_heat_net_flux_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:lightning_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:maximumcomposite_radar_reflectivity_entire_atmosphere:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:mean_sea_level_pressure_nam_model_reduction_msl:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:missing_pblri:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:number_of_soil_layers_in_root_zone_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:planetary_boundary_layer_height_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:plant_canopy_surface_water_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:precipitable_water_entire_atmosphere:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:pressure_maximum_wind:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:pressure_reduced_to_msl_msl:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:pressure_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:pressure_tropopause:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:relative_humidity_zerodegc_isotherm:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:sensible_heat_net_flux_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:snow_cover_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:snow_depth_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:soil_porosity_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:soil_type_as_in_zobler_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:surface_roughness_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:surface_wind_gust_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:temperature_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:temperature_tropopause:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:total_cloud_cover_entire_atmosphere:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:transpiration_stress-onset_soil_moisture_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:u-component_of_wind_maximum_wind:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:u-component_of_wind_pblri:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:u-component_of_wind_tropopause:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:upward_long_wave_rad_flux_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:upward_short_wave_rad_flux_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:v-component_of_wind_maximum_wind:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:v-component_of_wind_pblri:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:v-component_of_wind_tropopause:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:vegetation_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:vegitation_type_as_in_sib_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:visibility_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:water_equiv_of_accum_snow_depth_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
    -a "feature:wilting_point_surface:-.3,-.25,-.2,-.15,-.1,-.05,0,.05,.1,.15,.2,.25,.3" \
