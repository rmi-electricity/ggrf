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

Zillow %>% skim
Turbines %>% skim
Solar %>% skim
# For each zip code:
#		note the first and last occurrances of an object.
#		count time as t-5, during, and t+5


# Communities have, on avg, 40 turbines, built within 6.8 years.
Turbines %>%
	group_by(zip_code) %>%
	summarize(
		n = n(),
		first_date_of_operation = min(date_of_operation),
		last_date_of_operation  = max(date_of_operation),
	) %>%
	mutate(
		duration_days = difftime(last_date_of_operation, first_date_of_operation, units = 'days'),
		duration_years = lubridate::time_length(duration_days, 'years')
	) %>%
	skim(n, duration_years)

# Classify all zillow observation dates relative to 
# first and last dates of operation for new projects.

# First, for each zip code that has a turbine, 
# note all the relevant dates for the turbines- 
	# first date of operation, 
	# and last date of operation.
CteTurbineDates <-
	Turbines %>%
	group_by(zip_code) %>%
	summarize(
		first_date_of_operation = min(date_of_operation),
		last_date_of_operation =  max(date_of_operation),
	) %>%
	ungroup

# Solar
CteSolarDates <-
	Solar %>%
	group_by(zip_code) %>%
	summarize(
		first_date_of_operation = min(date_of_operation),
		last_date_of_operation =  max(date_of_operation),
	) %>%
	ungroup

# for each distinct zillow zip code:
# if it has a turbine/solar facility in it:
# for each date that has a home price observation:
	# note where we are vis a vis the turbine construction. options:
	# within five years before first start; within five years after last start;
	# between the first and last starts (construction), else 'censored'
classify_deltas <- function(Zillow, Manipulation){
	Zillow %>%
	distinct(zip_code, date) %>%
	inner_join(Manipulation, by = c('zip_code')) %>%
	mutate(
		delta_days = case_when(
			date < first_date_of_operation ~ difftime(date, first_date_of_operation, units='days'),
			date > last_date_of_operation  ~ difftime(date, last_date_of_operation, units='days'),
		),
		delta_years = round(lubridate::time_length(delta_days, 'years')),
	) %>%
	ungroup %>%
	mutate(
		manipulation = case_when(
			abs(delta_years) <= 5 ~ str_c(delta_years),
			(date >= first_date_of_operation) & (date <= last_date_of_operation) ~ 'Construction',
			TRUE ~ 'Censored'
		),
		manipulation = factor(manipulation, ordered=FALSE)
	)
}


CteManipulationTurbines <- classify_deltas(Zillow, CteTurbineDates) %>%
	select(zip_code, date, manipulation) %>%
	rename(turbine_manipulation = manipulation)

CteManipulationSolar <- classify_deltas(Zillow, CteSolarDates) %>%
	select(zip_code, date, manipulation) %>%
	rename(solar_manipulation = manipulation)

CteManipulationTurbines %>% count(turbine_manipulation)
CteManipulationSolar %>% count(solar_manipulation)

# Join manipulations together. If an area has a turbine but not a solar
# (or vice-versa), call its status 'censored'
Manipulation <-
	CteManipulationTurbines %>%
	full_join(CteManipulationSolar, by = c('zip_code', 'date')) %>%
	mutate(
		turbine_manipulation = fct_explicit_na(turbine_manipulation, 'Censored'),
		solar_manipulation = fct_explicit_na(solar_manipulation, 'Censored'),
		group = 'Manipulation'
	)


# Join the manipulation data to the home price dataset.
# any zipcode that doesn't have a turbine or solar manipulation should
# be noted as a control
JoinedData <-
	Zillow %>%
	select(zip_code, date, home_price) %>%
	left_join(Manipulation, by = c('zip_code', 'date')) %>%
	mutate(
		turbine_manipulation = fct_explicit_na(turbine_manipulation, 'Control'),
		solar_manipulation = fct_explicit_na(solar_manipulation, 'Control'),
		group = fct_explicit_na(group, 'Control')
	)
JoinedData %>% head

JoinedData %>%
	count(turbine_manipulation)
JoinedData %>%
	count(group)
JoinedData %>%
	count(solar_manipulation)
JoinedData %>% skim

# Note what's happening when one or the other project is 1 year from starting
JoinedData %>%
	filter(solar_manipulation==-1|turbine_manipulation==-1) %>%
	count(solar_manipulation, turbine_manipulation) %>%
	arrange(desc(n)) %>%
	as.data.frame


# Model
mod1 <- lmer(
	data = JoinedData,
	REML = FALSE,
	formula = home_price ~ date + group:solar_manipulation + group:turbine_manipulation + (1|zip_code)
)

mod0 <- lmer(
	data = JoinedData,
	REML = FALSE,
	formula = home_price ~ date + (1|zip_code)
)

anova(mod1, mod0)
mod1
levels <- c( str_c(seq(-5, -1)), 'Construction', str_c(seq(1, 5)), 'Censored', 'Control')
FixedEffects <-
	fixef(mod1) %>%
	enframe(name='fixef', value ='est') %>%
	filter(str_detect(fixef, 'solar')|str_detect(fixef, 'turbine')) %>%
	mutate(
		manipulation = case_when(
			str_detect(fixef, 'solar') ~ 'Solar',
			str_detect(fixef, 'turbine') ~ 'Turbine',
			TRUE ~ 'DEFAULT_UNKNOWN'
		),
		fixef = str_replace(fixef, '^.*manipulation', ''),
		fixef = ordered(fixef, levels),
		color = if_else(fixef %in% c('Construction', 'Censored'), 'Indeterminate state', 'Year')
	) %>%
	arrange(manipulation, fixef) 

FixedEffects %>%
	ggplot(aes(x = fixef, y = est, fill = color)) +
	geom_hline(yintercept = 0, color = 'white', width = 10000) +
	geom_col(position='dodge', width = 1) +
	scale_fill_manual(values = c('darkgrey', 'dodgerblue')) +
	scale_y_continuous(labels = scales::dollar_format()) +
	facet_wrap(~manipulation, nrow = 2) +
	labs(x = '', y = 'Estimate', fill = '', title = 'Fixed effects') +
	theme(legend.position = 'bottom')





Zillow %>%
	distinct(zip_code, date) %>%
	count(zip_code, name = 'num_observations') %>%
	skim(num_observations)
