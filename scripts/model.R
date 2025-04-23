library(tidyverse)
library(skimr)
library(arrow)
library(lme4)
library(sjstats)


fn_zillow_delta <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/zillow_delta.parquet'
fn_turbines <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/turbines.parquet'
fn_solar <- '/Volumes/Extreme SSD/ggrf_insurance/clean_data_2/solar.parquet'

ZillowDelta <- read_parquet(fn_zillow_delta) %>%
	rename(home_price_change = delta) %>%
	mutate(zip_code = factor(zip_code, ordered = FALSE))

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

#### Diagnostics ####

ZillowDelta %>% skim
Turbines %>% skim
Solar %>% skim

ZillowDelta %>%
	skim %>%
	yank('numeric') %>%
	as_tibble %>%
	rename(min = p0, max = p100) %>%
	select(skim_variable, n_missing, complete_rate, mean, sd, min, max)

ZillowDelta %>%
	skim %>%
	yank('factor') %>%
	as_tibble %>%
	select(skim_variable, n_missing, complete_rate, ordered, n_unique)

Turbines %>%
	skim %>% 
	yank('numeric') %>%
	as_tibble %>%
	rename(min = p0, max = p100) %>%
	select(skim_variable, n_missing, complete_rate, mean, sd, min, max)

Turbines  %>%
	skim %>% 
	yank('factor') %>% 
	as_tibble %>%
	select(skim_variable, n_missing, complete_rate, ordered, n_unique)

Solar %>%
	skim %>% 
	yank('numeric') %>%
	as_tibble %>%
	rename(min = p0, max = p100) %>%
	select(skim_variable, n_missing, complete_rate, mean, sd, min, max)

Solar %>%
	skim %>% 
	yank('factor') %>% 
	as_tibble %>%
	select(skim_variable, n_missing, complete_rate, ordered, n_unique)

# For each zip code:
#		note the first and last occurrances of an object.
#		count time as t-5, during, and t+5


# Communities have, on avg, 40 turbines, built within 6.8 years.
Turbines %>%
	group_by(zip_code) %>%
	summarize(
		n = n(),
		first_year_of_operation = min(year_of_operation),
		last_year_of_operation  = max(year_of_operation),
	) %>%
	ungroup %>%
	mutate(
		duration_years = last_year_of_operation - first_year_of_operation,
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
		first_year_of_operation = min(year_of_operation),
		last_year_of_operation  = max(year_of_operation),
	) %>%
	ungroup

# Solar
CteSolarDates <-
	Solar %>%
	group_by(zip_code) %>%
	summarize(
		first_year_of_operation = min(year_of_operation),
		last_year_of_operation  = max(year_of_operation),
	) %>%
	ungroup

# for each distinct zillow zip code:
# if it has a turbine/solar facility in it:
# for each date that has a home price observation:
	# note where we are vis a vis the turbine construction. options:
	# within five years before first start; within five years after last start;
	# between the first and last starts (construction), else 'censored'
classify_manipulation_state <- function(Zillow, Manipulation){
	ZillowDelta %>%
	distinct(zip_code, year) %>%
	inner_join(Manipulation, by = c('zip_code')) %>%
	mutate(
		delta_years = case_when(
			year < first_year_of_operation ~ year - first_year_of_operation,
			year > last_year_of_operation  ~ year - last_year_of_operation
		)
	) %>%
	ungroup %>%
	mutate(
		manipulation = case_when(
			abs(delta_years) <= 5 ~ str_c(delta_years),
			(year >= first_year_of_operation) & (year <= last_year_of_operation) ~ 'Construction',
			TRUE ~ 'Censored'
		),
		manipulation = factor(manipulation, ordered=FALSE)
	)
}


CteManipulationTurbines <-
	classify_manipulation_state(ZillowDelta, CteTurbineDates) %>%
	select(zip_code, year, manipulation) %>%
	rename(turbine_manipulation = manipulation)

CteManipulationSolar <-
	classify_manipulation_state(ZillowDelta, CteSolarDates) %>%
	select(zip_code, year, manipulation) %>%
	rename(solar_manipulation = manipulation)

CteManipulationTurbines %>% count(turbine_manipulation)
CteManipulationSolar %>% count(solar_manipulation)

# Join manipulations together. If an area has a turbine but not a solar
# (or vice-versa), call its status 'censored'
Manipulation <-
	CteManipulationTurbines %>%
	full_join(CteManipulationSolar, by = c('zip_code', 'year')) %>%
	mutate(
		turbine_manipulation = fct_explicit_na(turbine_manipulation, 'Censored'),
		solar_manipulation = fct_explicit_na(solar_manipulation, 'Censored'),
		group = 'Manipulation'
	)

Manipulation


# Join the manipulation data to the home price dataset.
# any zipcode that doesn't have a turbine or solar manipulation should
# be noted as a control
JoinedData <-
	ZillowDelta %>%
	select(zip_code, year, home_price_change) %>%
	left_join(Manipulation, by = c('zip_code', 'year')) %>%
	mutate(
		turbine_manipulation = fct_explicit_na(turbine_manipulation, 'Control'),
		solar_manipulation = fct_explicit_na(solar_manipulation, 'Control'),
		group = fct_explicit_na(group, 'Control'),
		year = factor(year, ordered = FALSE)
	)
JoinedData %>% head
JoinedData %>% is.na %>% colSums

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
# let's model year as a general categorical trend
mod1 <- lmer(
	data = JoinedData,
	REML = FALSE,
	formula = home_price_change ~ year + solar_manipulation + turbine_manipulation + (1|zip_code)
)

mod0 <- lmer(
	data = JoinedData,
	REML = FALSE,
	formula = home_price_change ~ factor(year, ordered = FALSE) + (1|zip_code)
)
anova(mod1, mod0)

# Goodness-of-fit Diagnostics

# residuals look sufficiently normally distributed,
# even if it doesn't pass the kolmogorov-smirnov test.
(residuals(mod1)) %>%
	enframe %>%
	ggplot(aes(x = value)) +
	geom_histogram() +
	labs(x = 'Residuals', y = 'n', title = 'Model residuals') +
	scale_y_continuous(labels = scales::comma_format())

ks.test(residuals(mod1), 'pnorm')

# ICC
# the adj. intraclass cor. coef. is 0.021, which 
# is rather low. this indicates that the fixed-effects
# are doing much more of the heavy-lifting in our model
# than the random effects (zip-codes) are, which is good
# i think.
icc <- performance::icc(mod1)
icc


#### Plot variables ####

CI <- confint.merMod(mod1, method="Wald") %>%
	as.data.frame %>%
	rownames_to_column() %>%
	rename(variable=1, `2.5%`=2, `97.5%`=3) %>%
	drop_na()

FixedEffects <-
	fixef(mod1, add.dropped=FALSE) %>%
	enframe(name = 'variable', value = 'estimate') %>%
	full_join(CI, by = 'variable')

all_levels <-
	c(
		seq(-5, -1),
		'Construction',
		seq(1, 5),
		'Censored',
		'Control',
		'(Intercept)',
		seq(2002, 2024)
	)

FixedEffectsClean <-
	FixedEffects %>%
		mutate(
			family = case_when(
				str_detect(variable, 'year') ~ 'General',
				str_detect(variable, 'Intercept') ~ 'General',
				str_detect(variable, 'solar') ~ 'Solar',
				str_detect(variable, 'turbine') ~ 'Turbine',
			),
			variable_adj = case_when(
				family == 'General' ~ str_replace(variable, 'year', ''),
				family == 'Solar' ~ str_replace(variable, 'solar_manipulation', ''),
				family == 'Turbine' ~ str_replace(variable, 'turbine_manipulation', ''),
			),
			variable_adj = ordered(variable_adj, all_levels),
			color = if_else(
				(`2.5%` > 0) | (`97.5%` < 0),
				'0.0 does not fall within the 95% CI',
				'0.0 falls within the 95% CI',
			)
			# variable_adj = fct_rev(variable_adj)
		) %>%
	filter(
		!str_detect(variable_adj, 'Control'),
		!str_detect(variable_adj, '(Intercept)'),
	)

FixedEffectsClean %>%
	filter(family == 'General') %>%
	ggplot(aes(x = variable_adj, y = estimate, ymin = `2.5%`, ymax = `97.5%`, color = color)) +
	scale_color_manual(values = c('dodgerblue', 'grey20')) +
	geom_point() +
	geom_linerange() +
	geom_hline(yintercept = 0) +
	labs(color = '', x = 'Variable', y = 'Estimate') +
	theme(legend.position = 'bottom', 
				axis.ticks = element_blank())

FixedEffectsClean %>%
	filter(family != 'General') %>%
	ggplot(aes(x = variable_adj, y = estimate, ymin = `2.5%`, ymax = `97.5%`, color = color)) +
	scale_color_manual(values = c('dodgerblue', 'grey20')) +
	geom_point() +
	geom_linerange() +
	geom_hline(yintercept = 0) +
	facet_wrap(~family, ncol = 1) +
	labs(color = '', x = 'Variable', y = 'Estimate') +
	theme(legend.position = 'bottom', 
				axis.ticks = element_blank())


# What does overall change in value look like over time?
# JoinedData %>%
# 	mutate(year = ordered(year)) %>%
# 	ggplot(aes(x = year, y = home_price_change)) +
# 	geom_hline(yintercept = 0) +
# 	geom_boxplot(outlier.alpha = 0.5) +
# 	labs(x = 'Year', y = 'Home price change from last year') +
# 	scale_y_continuous(limits = c(-0.75, 0.75), breaks = seq(-0.75, 0.75, 0.25))
