# Get a gander at where our sampled zip-codes are

library(tidyverse)
library(skimr)
library(arrow)
library(sf)

fn_zips <- '/Volumes/Extreme SSD/ggrf_insurance/input_data/zcta/tl_2020_us_zcta520.shp'
fn_turbines <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/turbines.parquet'
fn_solar <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/solar.parquet'
fn_zillow_delta <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/zillow_delta.parquet'

dir_out <- '/Users/andrewbartnof/Documents/rmi/ggrf_redux/writeup'
fn_continental <- file.path(dir_out, 'zip_code_centroids_continental.png')
fn_all <- file.path(dir_out, 'zip_code_centroids_all.png')



Zips <-
	read_sf(fn_zips) %>%
	rename(zip_code = ZCTA5CE20) %>%
	select(zip_code, geometry) %>%
	st_centroid()

Turbines <- read_parquet(fn_turbines) %>%
	select(zip_code) %>%
	mutate(is_turbine = TRUE) %>%
	distinct

Solar <- read_parquet(fn_solar) %>%
	as_tibble %>%
	select(zip_code) %>%
	mutate(is_solar = TRUE) %>%
	distinct

Zillow <- read_parquet(fn_zillow_delta) %>%
	select(zip_code) %>%
	mutate(is_zillow = TRUE) %>%
	distinct

CteZipToGroup <-
	Zips %>%
	st_drop_geometry() %>%
	left_join(Zillow, by = 'zip_code') %>%
	left_join(Turbines, by = 'zip_code') %>%
	left_join(Solar, by = 'zip_code') %>%
	mutate_at(c('is_zillow', 'is_turbine', 'is_solar'), replace_na, FALSE) %>%
	mutate(
		group = case_when(
			is_zillow & !is_turbine & !is_solar ~ 'Control',
			is_zillow & is_turbine & !is_solar ~ 'Manipulation: Turbine',
			is_zillow & !is_turbine & is_solar ~ 'Manipulation: Solar',
			is_zillow & is_turbine & is_solar ~ 'Manipulation: Turbine & Solar',
			!is_zillow ~ '(Unknown)',
		),
		group2 = case_when(
			is_zillow & (is_turbine | is_solar) ~ 'Manipulation',
			is_zillow ~ 'Control',
			TRUE ~ '(Unknown)'
		)
	) %>%
	select(zip_code, group, group2)

Control <-
	Zips %>%
	inner_join(CteZipToGroup, by = 'zip_code') %>%
	filter(group2 == 'Control')
Manipulation <-
	Zips %>%
	inner_join(CteZipToGroup, by = 'zip_code') %>%
	filter(group2 == 'Manipulation')

sf_use_s2(FALSE)
g <-
	Control %>%
	ggplot() +
	geom_sf(color = 'grey') +
	geom_sf(data = Manipulation, color = 'maroon') +
	coord_sf(xlim = c(-130, -60), ylim = c(23.7, 50)) +
	labs(title = 'Distribution of data points', subtitle = 'All zip-codes represented as points\nGrey indicates control, and maroon indicates manipulation\nSubsetted to continental USA') +
	theme_void() +
	theme(text = element_text(family = 'serif'))
g
ggsave(filename = fn_continental, plot = g, width = 8, height = 8)


sf_use_s2(FALSE)
g <-
	Control %>%
	ggplot() +
	geom_sf(color = 'grey') +
	geom_sf(data = Manipulation, color = 'maroon') +
	# coord_sf(xlim = c(-130, -60), ylim = c(23.7, 50)) +
	labs(title = 'Distribution of data points', subtitle = 'All zip-codes represented as points\nGrey indicates control, and maroon indicates manipulation\nAll 50 states') +
	theme_void() +
	theme(text = element_text(family = 'serif'))
g
ggsave(filename = fn_all, plot = g, width = 8, height = 8)
