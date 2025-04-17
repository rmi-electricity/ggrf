# Perform ELT on the turbines table, zillow house values table, and the 
# zip-codes table. Save the resulting 'clean' files to a subfolder

library(tidyverse)
library(skimr)
library(arrow)
library(sf)
library(units)

epsg_standard <- 4326  # Conform all coordinates to this projection

fn_zillow <- '/Volumes/Extreme SSD/ggrf_insurance/input_data/zillow_sm_sa_month.csv'
fn_zillow_clean <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/zillow_clean.parquet'

fn_turbines <- '/Volumes/Extreme SSD/ggrf_insurance/input_data/uswtdbSHP/uswtdb_V8_0_20250225.shp'
# fn_turbines <- '/Volumes/Extreme SSD/ggrf_insurance/input_data/us_wind_turbine_data/uswtdb_v7_0_20240510.csv'
fn_turbines_clean <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/turbines_clean.RDS'

fn_zips <- '/Volumes/Extreme SSD/ggrf_insurance/input_data/zcta/tl_2020_us_zcta520.shp'
fn_zips_clean <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/zip_to_coord.RDS'

fn_distance <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/distance.parquet'



# Read Zillow data-- add a row for observation_id, and keep only zip-code and
# house price on a given date. Convert this to 'long' format.

Zillow <- read_csv(fn_zillow)

ZillowClean <-
	Zillow %>%
	rowid_to_column('zillow_observation_id') %>%
	select(-RegionID, -SizeRank, -RegionType, -StateName, -State, -City, -Metro, -CountyName) %>%
	rename(zip_code = RegionName) %>%
	gather(date, home_price, -zillow_observation_id, -zip_code) %>%
	mutate(date = lubridate::ymd(date))

ZillowClean %>% write_parquet(fn_zillow_clean)
# rm(Zillow, ZillowClean)

# Turbines
# Note each turbine's unique id; convert year of operation to a 'date' object;
# save long and lat

Turbines <- sf::read_sf(fn_turbines)
Turbines <- st_transform(Turbines, crs = epsg_standard)
TurbinesClean <-
	Turbines %>%
	select(case_id, p_year, geometry) %>%
	rename(turbine_id = case_id, turbine_year_of_operation = p_year, turbine_coords = geometry) %>%
	mutate(
		turbine_date_of_operation = lubridate::ymd(str_c(turbine_year_of_operation), truncated = 2L),
		turbine_id = factor(turbine_id, ordered = FALSE)
	) %>%
	relocate(turbine_id, turbine_date_of_operation, turbine_coords) %>%
	arrange(turbine_id, turbine_date_of_operation)

TurbinesClean %>% write_rds(fn_turbines_clean)


# Zip Codes
# Keep zip code name, lat, and longitude

Zips <- sf::read_sf(fn_zips)
Zips <- st_transform(Zips, crs = epsg_standard)

# Create centroids manually instead of using the pre-presented centroids
# because we're re-projecting the coordinates, and it's easy enough to 
# transform the polygons and THEN get coordinates.
ZipsClean <-
	Zips %>%
	select(ZCTA5CE20, geometry) %>%
	st_centroid() %>%
	rename(zip_code = ZCTA5CE20, zip_centroid = geometry) %>%
	arrange(zip_code)

ZipsClean %>%
	write_rds(fn_zips_clean)


# NOTE DISTANCE FROM ZILLOW LOCATIONS TO NEAREST RELEVANT TURBINES
#   for each zip code in the zillow dataset:
#     find the distance to all turbines
#     if any turbines are within our cutoff (5km right now),
#     collect the first five turbines to have been installed.
# NB this takes a few hours, but we only have to do it once.

# Collect each unique zip code from the zillow dataset. Filter out any that
# don't exist in the zip code table (zip codes change over time).
zip_codes_to_test <-
	ZillowClean %>%
	distinct(zip_code) %>%
	semi_join(ZipsClean, by = 'zip_code') %>%
	pull(zip_code)

all(zip_codes_to_test %in% ZipsClean$zip_code)
cutoff_m <- 5 * 1000 # 5 km

# geometry 2 is always all of the turbine locations
g2 <-
	TurbinesClean %>%
	select(turbine_coords)

# We'll want to strip geography from the turbine table so we can 
# join date to id easily.
TurbineIdToDate <-
	TurbinesClean %>%
	as.data.frame %>%
	select(turbine_id, turbine_date_of_operation) 

i_range <- seq(length(zip_codes_to_test))
CollectedResults <- tibble()
for (i in i_range){
	target_zip_code <- zip_codes_to_test[i]
	
	g1 <-
		ZipsClean %>% 
		filter(zip_code == target_zip_code)
	
	# note that units is denoted in the projection of each geometry file
	# https://stackoverflow.com/questions/70436019/extracting-the-measurement-unit-degrees-metres-etc-from-spatial-data-in-r
	distance_m <- st_distance(g1, g2, by_element = TRUE)
	distance_m <- as.integer(distance_m)
	
	Results <-
		tibble(
			turbine_id = TurbinesClean$turbine_id,
			zillow_zip_code = target_zip_code,
			distance_m
		) %>%
		left_join(TurbineIdToDate, by = 'turbine_id')

	# Within the distance cutoff,
	# collect the first five turbines to have been built 
	# (allow ties, so actually we can get more than 5)
	Min <-
		Results %>%
		filter(distance_m <= cutoff_m) %>%
		slice_min(turbine_date_of_operation, n = 5)
	
	print( sprintf('%i of %i, %i rows appended', i, length(zip_codes_to_test), nrow(Min)) )
	CollectedResults <- CollectedResults %>% bind_rows(Min)
}

CollectedResults %>% write_parquet(fn_distance)