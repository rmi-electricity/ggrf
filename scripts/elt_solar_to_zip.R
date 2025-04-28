# ELT on solar data, link each observation to a zip-code

# Perform the kinds of essential ELT that has to happen (eg matching locations
# of turbines to zip-codes), but leave feature engineering 
# (eg leaving observations as-is, or aggregating them) to the individual model 
# scripts.

library(tidyverse)
library(sf)
library(skimr)
library(arrow)

epsg_standard <- 4326  # Conform all coordinates to this projection

fn_solar_sf <- '/Volumes/Extreme SSD/ggrf_insurance/input_data/united_states_large_scale_solar_photovoltaic_database/uspvdbSHP/uspvdb_v2_0_20240801.shp'
fn_zips <- '/Volumes/Extreme SSD/ggrf_insurance/input_data/zcta/tl_2020_us_zcta520.shp'

fn_solar_out <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/solar.parquet'


# Load files
Zips <- sf::read_sf(fn_zips)
Zips <- st_transform(Zips, crs = epsg_standard)
ZipPoints <-
	Zips %>%
	select(ZCTA5CE20) %>%
	rename(zip_code = ZCTA5CE20, zip_point = geometry) %>%
	st_centroid()

Solar <- sf::read_sf(fn_solar_sf)
old_solar_basegeocrs <- 'NAD83'
SolarPoints <-
	Solar %>%
	as.data.frame %>%
	select(eia_id, ylat, xlong) %>%
	rename(solar_eia_id = eia_id) %>%
	st_as_sf(., coords = c('xlong', 'ylat'), remove=TRUE, crs=old_solar_basegeocrs)
SolarPoints <- st_transform(SolarPoints, crs = epsg_standard)

#	Iterate through each solar panel, note the zip code centroid that's closest.
# Record these zip codes in a list.
# Note that this takes a while to run
matches_list <- rep('', nrow(SolarPoints))
for (i in seq(1, nrow(SolarPoints))){
	print(sprintf('%i of %i: %.3f', i, nrow(SolarPoints), i / nrow(SolarPoints)))
	target_solar <- SolarPoints$solar_eia_id[i]
	g1 <- SolarPoints %>% slice(i)
	
	# find the index of the min. distance
	distance <- st_distance(g1, ZipPoints, by_element = FALSE)
	j <- which.min(distance)
	matched_zip_code <- ZipPoints$zip_code[j]
	
	# Store best zip code in list at position i
	matches_list[i] <- matched_zip_code
}

SolarClean <-
	Solar %>%
	as.data.frame %>%
	select(eia_id, p_year) %>%
	mutate(
		date_of_operation = lubridate::ymd(str_c(p_year), truncated=2L),
		p_year = NULL
	) %>%
	bind_cols(zip_code = matches_list)

SolarClean %>% write_parquet(fn_solar_out)