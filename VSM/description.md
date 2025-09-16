Document title: The VSM cleaup tool - cleanup concepts 

**Secure folder**
The cleanup is oriented around the concept of a root folder with owners and approvers that represents a teams effort to accomplish a well defined purpose.

**Cleanup cycle** 
The purpose of the cleanup cycle is to ensure that the owner  and approvers of the secure folder has time to change the retention of simulations that are marked for cleanup. 
At the end of the cleanup cycle the simulations still marked for cleanup will be clean.

The cleanup cycle is run on secure folder by a scheduler according to the secure folders cleanup_frequency.


**How to get the user started on clean-up of a secure folder**
A secure folders always starts in the a state with cleanup_frequency="inactive" meaning that the cleanup process is inactive 

The owner or approvers of the secure folder selects the cleanup frequency that they expect will work for their project: 15 days, 30 days, 45 days

The cleanup frequency can be changed or paused (cleanup_frequency="inactive"). 
⦁	Activating the cleanup activity will define retention_expiration_dates as explained below. 
⦁	Resuming will use the previous defined retention_expiration_dates.
The simulation' retention_expiration_date is used to calculate the retention states so teh user change retnetion of hundreds or thousenad of simulations with little effort in the cleanup' tool UI


**Dates and count of days**:
⦁	cleanup_status_date: marks the day a new cleanup round starts. it will stay the same until a new cleanup round starts. Using the cleanup_status_date we will be able to classify new simulations that starts after cleanup_status_date as in the retention state "new" instead of "marked". Cleanup of simulations with retention "new" will be postponed until the next cleanup round where their status will change to "marked" or path, clean, issue etc. 
⦁	modified_date: the date the simulation was last modified
⦁	retention_expiration_date: is the date the simulation can be marked for in a new cleanup round
	simulations under "path" protection: the retention_expiration_date is ignored
	new simulations: created after the current cleanup_status_date starts with retention_expiration_date = cleanup_status_date+2*cleanup_frequency-1. -1 to ensure the simulation becomes part of the next cleanup round
 	other simulations: the  user controls the days to cleanup using the retention page. 
		fx a change to 90d sets the retention_expiration_date = cleanup_status_date+90d
		fx next sets the retention_expiration_date = cleanup_status_date+2*cleanup_frequency

**retention states**
So far we operate with the following retentions. all but the mandatory state can be changed

retention_catalog : (key=retention_label, value=days_to_cleanup)
⦁	marked, cleanup_frequency: mandatory state for simulations marked for cleanup at the end of the current cleanup round. changing to this retention sets retention_expiration_date = 2*cleanup_frequency-1 day
⦁	new, 2*cleanup_frequency:  mandatory state for new simulation created in the current cleanup round unless they are path protected. Simulations in this state will be marked for cleanup in the next cleanup round. changing to this retention sets retention_expiration_date = 2*cleanup_frequency-1
⦁	90d,     90: changing to this retention sets retention_expiration_date = cleanup_status_date+90 days
⦁	180d,   180: changing to this retention sets retention_expiration_date = cleanup_status_date+180 days
⦁	365d,   365: changing to this retention sets retention_expiration_date = cleanup_status_date+365 days
⦁	730d,   730: changing to this retention sets retention_expiration_date = cleanup_status_date+730 days
⦁	1095d, 1095: changing to this retention sets retention_expiration_date = cleanup_status_date+1095 days

**The retention state can be calculated from the following paramters**
⦁	path : this is the main retention parameter ALL simulation under the path must be classified as path protected even if there is nothing to clean or teh simulation has issues such a reproducibilit, missing or multiple set files
⦁	clean: if the simulation was clean by the system or other tools then set the retention to clean
⦁	issue: if the simulation cannot be cleaned due to set file issues, or lack of reproducibility depending on LaC' opinion on this

The calculation of the retention of the remaining simulations is based on days_to_cleanup = retention_expiration_date - cleanup_status_date : 
⦁	marked: days_to_cleanup <= cleanup_frequency
⦁	new:    cleanup_frequency < days_to_cleanup <= 2*cleanup_frequency
⦁	 90d :  2*cleanup_frequency < days_to_cleanup <= 90 days
⦁	180d :  90  < days_to_cleanup <= 180 
⦁	365d :  180 < days_to_cleanup <= 365 
⦁	730d :  365 < days_to_cleanup <= 730 
⦁	1095d : 730 < days_to_cleanup 



the_simulations_days_to_cleanup
i=-1
cat_days_to_cleanup = retention_catalog.values
while( ++i < len(cat_days_to_cleanup) && cat_days_to_cleanup[i] <= the_simulations_days_to_cleanup )
cat_retention_label=retention_category[i].key
