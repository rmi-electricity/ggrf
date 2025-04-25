library(tidyverse)
library(skimr)
library(arrow)
# library(lme4)


fn_zillow <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/zillow_clean.parquet'
fn_turbines <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/turbines.parquet'
fn_solar <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/solar.parquet'


# images
dir_out <- '/Users/andrewbartnof/Documents/rmi/ggrf_redux/writeup'
fn_solar_facets <- file.path(dir_out, 'study1_solar_facets.png')
fn_turbines_facets <- file.path(dir_out, 'study1_turbine_facets.png')
fn_sample_size <- file.path(dir_out, 'study1_sample_size.png')

Zillow <-
	read_parquet(fn_zillow) %>%
	mutate(
		date = lubridate::round_date(date, unit='year'),
		year = as.integer(lubridate::year(date))
	) %>%
	select(year, zip_code, home_price) %>%
	drop_na %>%
	group_by(year, zip_code) %>%
	summarize(home_price = median(home_price)) %>%
	ungroup
	
Turbines <-
	read_parquet(fn_turbines) %>%
	as_tibble %>%
	filter(turbine_date_of_operation <= lubridate::ymd('2025-01-01')) %>%
	mutate(
		year_of_operation = lubridate::round_date(turbine_date_of_operation, 'year'),
		year_of_operation = lubridate::year(turbine_date_of_operation),
		year_of_operation = as.integer(year_of_operation)
	) %>%
	select(turbine_id, zip_code, year_of_operation) %>%
	rename(id = turbine_id) %>%
	mutate_at(c('id', 'zip_code'), factor, ordered=FALSE)


Solar <-
	read_parquet(fn_solar) %>%
	as_tibble %>%
	filter(date_of_operation <= lubridate::ymd('2025-01-01')) %>%
	mutate(
		year_of_operation = lubridate::round_date(date_of_operation, 'year'),
		year_of_operation = lubridate::year(date_of_operation),
		year_of_operation = as.integer(year_of_operation)
	) %>%
	select(eia_id, zip_code, year_of_operation) %>%
	rename(id = eia_id) %>%
	mutate_at(c('id', 'zip_code'), factor, ordered=FALSE)

print(sprintf('%i zip codes with both manipulations', length(intersect(Turbines$zip_code, Solar$zip_code))))
print(sprintf('%i zip codes with only turbines', length(setdiff(Turbines$zip_code, Solar$zip_code))))
print(sprintf('%i zip codes with only solar', length(setdiff(Solar$zip_code, Turbines$zip_code))))

print(sprintf('%i control zip codes',
	length(setdiff(
		Zillow$zip_code,
		union(Turbines$zip_code, Solar$zip_code)
	))
))

year_list <- unique(Zillow$year)
year_list


# Paired Matches
# For starters, let's look at areas that had:
# 1 solar manipulation, no turbines
# year of operation == 2015


#### DID for Solar ####
CollectedSolar <- tibble()
for (target_year in seq(2008L, 2018L)){
	# MANIPULATION ZIP CODES	
	# these are zip codes that had one manipulation 
	# (not solar and turbine, nor two of either),
	# on the target_year
	ManipulationZips <-
		Solar %>%
		anti_join(Turbines, by='zip_code') %>%
		add_count(zip_code, name = 'num_manipulations') %>%
		filter(
			num_manipulations == 1L,
			year_of_operation == target_year
		) %>%
		select(zip_code) %>%
		mutate(group = 'Manipulation')
	num_manipulations <- nrow(ManipulationZips)
	print(sprintf('%i has %i manipulation observations', target_year, num_manipulations))

	# CONTROL ZIP CODES	
	# Any zip code that has no solar nor turbine can be a comparison
	# Make sure the zipcode has a yearly value per year from 6 years prior to
	# 5 years after
	start_year <- target_year - 6L
	end_year <- target_year + 5L
	num_years_in_seq = length(seq(start_year, end_year))
	
	ControlZips <-
		Zillow %>%
		anti_join(Turbines, by = 'zip_code') %>%
		anti_join(Solar, by = 'zip_code') %>%
		distinct(year, zip_code) %>%
		filter(year >= start_year, year <= end_year) %>%
		add_count(zip_code, name = 'obs_per_zip') %>%
		filter(obs_per_zip == num_years_in_seq) %>%
		distinct(zip_code) %>%
		mutate(group = 'Control')
	
	# Join control and manipulation, filter to +/- five years of the event.	
	JoinedData <-
		bind_rows(
			Zillow %>% inner_join(ControlZips, by = 'zip_code'),
			Zillow %>% inner_join(ManipulationZips, by = 'zip_code')
		) %>%
		filter(between(x = year, left = start_year, right = end_year)) %>%
		mutate(target_year = target_year)

	# Calculate price change, year to year. Replace first year with 0		
	PriceDelta <-
		JoinedData %>%
		arrange(zip_code, year) %>%
		group_by(zip_code) %>%
		mutate(
			price_delta = (home_price - lag(home_price))/lag(home_price),
			price_delta = replace_na(price_delta, 0.0)
		) %>%
		ungroup
	CollectedSolar <- bind_rows(PriceDelta, CollectedSolar)
}


#### DID for Turbines ####
CollectedTurbines <- tibble()
for (target_year in seq(2008L, 2018L)){
	# MANIPULATION ZIP CODES	
	# these are zip codes that had one manipulation 
	# (not solar and turbine, nor two of either),
	# on the target_year
	ManipulationZips <-
		Turbines %>%
		anti_join(Solar, by='zip_code') %>%
		add_count(zip_code, name = 'num_manipulations') %>%
		filter(
			num_manipulations == 1L,
			year_of_operation == target_year
		) %>%
		select(zip_code) %>%
		mutate(group = 'Manipulation')
	num_manipulations <- nrow(ManipulationZips)
	print(sprintf('%i has %i manipulation observations', target_year, num_manipulations))

	# CONTROL ZIP CODES	
	# Any zip code that has no solar nor turbine can be a comparison
	# Make sure the zipcode has a yearly value per year from 6 years prior to
	# 5 years after
	start_year <- target_year - 6L
	end_year <- target_year + 5L
	num_years_in_seq = length(seq(start_year, end_year))
	
	ControlZips <-
		Zillow %>%
		anti_join(Turbines, by = 'zip_code') %>%
		anti_join(Solar, by = 'zip_code') %>%
		distinct(year, zip_code) %>%
		filter(year >= start_year, year <= end_year) %>%
		add_count(zip_code, name = 'obs_per_zip') %>%
		filter(obs_per_zip == num_years_in_seq) %>%
		distinct(zip_code) %>%
		mutate(group = 'Control')
	
	# Join control and manipulation, filter to +/- five years of the event.	
	JoinedData <-
		bind_rows(
			Zillow %>% inner_join(ControlZips, by = 'zip_code'),
			Zillow %>% inner_join(ManipulationZips, by = 'zip_code')
		) %>%
		filter(between(x = year, left = start_year, right = end_year)) %>%
		mutate(target_year = target_year)

	# Calculate price change, year to year. Replace first year with 0		
	PriceDelta <-
		JoinedData %>%
		arrange(zip_code, year) %>%
		group_by(zip_code) %>%
		mutate(
			price_delta = (home_price - lag(home_price))/lag(home_price),
			price_delta = replace_na(price_delta, 0.0)
		) %>%
		ungroup
	CollectedTurbines <- bind_rows(PriceDelta, CollectedTurbines)
}


AggregatedByYear <-
	bind_rows(
		CollectedSolar %>% mutate(manipulation = 'Solar'),
		CollectedTurbines %>% mutate(manipulation = 'Turbines')
	) %>%
	mutate(
		relative_year = year - target_year,
		year = ordered(year)
	) %>%
	filter(relative_year != -6) %>%
	group_by(manipulation, target_year, group, relative_year) %>%
	summarize(median_price_delta = median(price_delta)) %>%
	ungroup
AggregatedByYear %>%
	count(manipulation, target_year, group) %>%
	pull(n) %>%
	all(. == 11L)

#
g <-
AggregatedByYear %>%
filter(manipulation == 'Solar') %>%
ggplot(aes(x = ordered(relative_year), y = median_price_delta, group = group, color = group)) +
geom_hline(yintercept = 0, linetype = 'dashed') +
geom_vline(xintercept = '0', linetype = 'dashed') +
geom_line() +
scale_color_manual(values = c('grey20', 'dodgerblue')) +
facet_wrap(~target_year) +
theme(
	legend.position = 'bottom',
	axis.ticks = element_blank(),
	text = element_text(family = 'serif')
) +
labs(
	color = '', 
	x = 'Relative year', 
	y = 'Median change in home prices',
	title = 'Solar'
)
g
ggsave(filename = fn_solar_facets, plot = g, width = 8, height = 8)

g <-
AggregatedByYear %>%
filter(manipulation == 'Turbines') %>%
ggplot(aes(x = ordered(relative_year), y = median_price_delta, group = group, color = group)) +
geom_hline(yintercept = 0, linetype = 'dashed') +
geom_vline(xintercept = '0', linetype = 'dashed') +
geom_line() +
scale_color_manual(values = c('grey20', 'dodgerblue')) +
facet_wrap(~target_year) +
theme(
	legend.position = 'bottom',
	axis.ticks = element_blank(),
	text = element_text(family = 'serif')
) +
labs(
	color = '', 
	x = 'Relative year', 
	y = 'Median change in home prices',
	title = 'Turbines'
)
g
ggsave(filename = fn_turbines_facets, plot = g, width = 8, height = 8)
#	
		



MedianPriceDelta <-
	bind_rows(
		CollectedSolar %>% mutate(manipulation = 'Solar'),
		CollectedTurbines %>% mutate(manipulation = 'Turbines')
	) %>%
	mutate(relative_year = year - target_year) %>%
	group_by(manipulation, group, relative_year) %>%
	summarize(
		median_price_delta = median(price_delta),
		# sd = sd(price_delta)
	) %>%
	ungroup %>%
	filter(relative_year > -6)
#	
MedianPriceDelta %>%
	spread(group, median_price_delta) %>%
	mutate(is_control_greater = Control > Manipulation) %>%
	ggplot(aes(x = relative_year)) +
	geom_hline(yintercept = 0, linetype = 'dotted') +
	geom_vline(xintercept = 0, linetype = 'dotted') +
	geom_segment(
		aes(xend = relative_year, y = Control, yend = Manipulation, color = is_control_greater), size = 1) +
		# arrow = arrow(length = unit(0.5, "cm"), type='closed')) +
	geom_label(aes(y = Control), color = 'grey20', label = 'C', label.r =  unit(.5, "lines")) +
	geom_label(aes(y = Manipulation), color = 'dodgerblue', label = 'M', label.r =  unit(.5, "lines")) +
	# geom_point(aes(y = Control), color = 'grey20', shape = 'C') +
	# geom_point(aes(y = Manipulation), color = 'dodgerblue', shape = 'M') +
	scale_color_manual(values = c('dodgerblue', 'grey20')) +
	scale_x_continuous(breaks = seq(-5, 5), labels = c(-5, -4, -3, -2, -1, 'Operational', 1, 2, 3, 4, 5)) +
	scale_y_continuous(labels = scales::percent_format()) +
	facet_wrap(~manipulation) +
	theme(
		legend.position = 'none',
		axis.ticks.x = element_blank(),
		panel.grid.minor.x = element_blank()
	) +
	labs(x = 'Relative year', y = 'Yearly appreciation', title = 'Property appreciation v previous year', 
			 subtitle = 'When manipulation (M) is higher than control (C),\nthen properties in the same zip-code as a green energy project have a higher increase in property value\nvis-a-vis the previous year') 

# Note that we have some pretty small sample sizes here
g <-
	bind_rows(
		CollectedSolar %>% mutate(manipulation = 'Solar'),
		CollectedTurbines %>% mutate(manipulation = 'Turbines')
	) %>%
	count(year, manipulation, group, name = 'num_observations') %>%
	ggplot(aes(x = year, y = num_observations, fill = group, group = group, label = scales::comma(num_observations))) +
	geom_col(position = 'dodge') +
	scale_fill_manual(values = c('grey20', 'dodgerblue')) +
	facet_wrap(group~manipulation, scales = 'free_y') + 
	scale_y_continuous(labels = scales::comma_format()) +
	theme(
		legend.position = 'none',
		axis.ticks = element_blank(),
		text = element_text(family = 'serif')
	) +
	labs(x = 'Target year of operation', y = 'Sample size', title = 'Sample sizes')
g	
ggsave(filename = fn_sample_size, plot = g, width = 8, height = 8)
#








CollectedResults %>%
	mutate(
		relative_year = year - target_year,
		relative_year = ordered(relative_year)
	) %>%
	group_by(relative_year, group) %>%
	summarize(median_price_delta = median(price_delta)) %>%
	ungroup %>%
	spread(group, median_price_delta) %>%
	mutate(is_control_greater = Control > Manipulation) %>%
	filter(relative_year > -6) %>%
	ggplot(aes(x = relative_year, xend = relative_year, y = Control, yend = Manipulation, color = is_control_greater)) + 
	geom_segment(arrow = arrow(length = unit(0.5, "cm")))
	

	# ggplot(aes())
	
	
	
CollectedResults %>%
	mutate(
		relative_year = year - target_year,
		relative_year = ordered(relative_year)
	) %>%
	group_by(relative_year, group) %>%
	summarize(median_price_delta = median(price_delta)) %>%
	ungroup %>%
	ggplot(aes(x = relative_year, fill = group, group = group, y = median_price_delta)) +
	geom_col(position = 'dodge') +
	scale_fill_manual(values = c('grey20', 'dodgerblue'))

CollectedResults %>%
	mutate(relative_year = year - target_year) %>%
	group_by(group, relative_year) %>%
	summarize(
		median_price_delta = median(price_delta),
		low = median_price_delta - sd(price_delta),
		high = median_price_delta + sd(price_delta),
	) %>%
	ungroup %>%
	ggplot(aes(x = relative_year, y = median_price_delta, group = group, color = group)) +
	geom_line() +
	geom_line(aes(y = low), linetype = 'dashed') +
	geom_line(aes(y = high), linetype = 'dashed')


PriceDelta %>%
	group_by(year, group) %>%
	summarize(
		median_price_delta = median(price_delta),
		low = median(price_delta) - sd(price_delta),
		high = median(price_delta) + sd(price_delta),
	) %>%
	ungroup %>%
	ggplot(aes(x = year, y = median_price_delta, ymin = low, ymax = high, group = group , color = group)) +
	geom_vline(xintercept = 2015) +
	geom_line()
