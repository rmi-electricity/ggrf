library(tidyverse)
library(skimr)
library(arrow)
library(lme4)


fn_zillow <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/zillow_clean.parquet'
fn_turbines <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/turbines.parquet'
fn_solar <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/solar.parquet'

Zillow <- read_parquet(fn_zillow) %>%
	mutate(date = lubridate::round_date(date, unit='year')) %>%
	drop_na(date, zip_code, home_price)

Turbines <- read_parquet(fn_turbines) %>%
	as_tibble %>%
	rename(id = turbine_id, date_of_operation = turbine_date_of_operation) %>%
	filter(date_of_operation <= lubridate::ymd('2025-01-01')) %>%
	mutate_at(c('id', 'zip_code'), factor, ordered=FALSE) %>%
	mutate(date_of_operation = lubridate::round_date(date_of_operation, unit='year'))
Solar <-
	read_parquet(fn_solar) %>% 
	as_tibble %>%
	rename(id = eia_id) %>%
	filter(date_of_operation <= lubridate::ymd('2025-01-01')) %>%
	mutate_at(c('id', 'zip_code'), factor, ordered=FALSE) %>%
	mutate(date_of_operation = lubridate::round_date(date_of_operation, unit='year'))
