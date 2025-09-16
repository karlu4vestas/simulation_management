Document title: The VSM tools - central concepts 

# VSM Cleanup Process Documentation

## Overview

The VSM (Vestas Simulation Management) system implements a comprehensive cleanup process designed to manage simulation data lifecycle efficiently. This document outlines the key concepts, processes, and technical specifications for the cleanup functionality.

## Core Concepts

### Secure Folder

The cleanup is oriented around the concept of a root folder with owners and approvers that represents a team's effort to accomplish a well-defined purpose.

### Cleanup Cycle

The purpose of the cleanup cycle is to ensure that the owner and approvers of the secure folder have time to change the retention of simulations that are marked for cleanup. At the end of the cleanup cycle, the simulations still marked for cleanup will be cleaned.

The cleanup cycle is run on each secure folder by a scheduler according to the secure folder's cleanup frequency.


## Getting Started with Cleanup

### Initial Setup

A secure folder always starts in a state with `cleanup_frequency="inactive"`, meaning that the cleanup process is inactive.

The owner or approvers of the secure folder select the cleanup frequency that they expect will work for their project: 15 days, 30 days, or 45 days. These choices can be adapted.

### Managing Cleanup Frequency

The cleanup frequency can be changed or paused (`cleanup_frequency="inactive"`):

- **Activating** the cleanup activity will define retention expiration dates as explained below
- **Resuming** will use the previously defined retention expiration dates

The simulation's `retention_expiration_date` is used to calculate the retention states, so the user can change retention of hundreds or thousands of simulations with little effort in the cleanup tool UI.


## Date Management and Calculations

### Key Date Fields

- **`cleanup_status_date`**: Marks the day a new cleanup round starts. It will stay the same until a new cleanup round starts. 
Using the `cleanup_status_date`, and below paramters we will be able to classify the retention category of old and new simulations.

- **`modified_date`**: The date the simulation was last modified

- **`retention_expiration_date`**: The date when the simulation can be marked for cleanup in a new cleanup round

    
### Available Retention Categories

We operate with the following retention states. All but the non-mandatory states can be changed if needed.

**Retention Catalog** (key=retention_label, value=days_to_cleanup):

- **`marked`** (cleanup_frequency days): Mandatory state for simulations marked for cleanup at the end of the current cleanup round. Changing to this retention sets `retention_expiration_date = cleanup_status_date + cleanup_frequency` days

- **`next`** (2 × cleanup_frequency days): Mandatory state for new simulations created in the current cleanup round unless they are path protected. Simulations in this state will be marked for cleanup in the next cleanup round. Changing to this retention sets `retention_expiration_date = cleanup_status_date + 2 × cleanup_frequency`

- **`90d`** (90 days): Changing to this retention sets `retention_expiration_date = cleanup_status_date + 90 days`

- **`180d`** (180 days): Changing to this retention sets `retention_expiration_date = cleanup_status_date + 180 days`

- **`365d`** (365 days): Changing to this retention sets `retention_expiration_date = cleanup_status_date + 365 days`

- **`730d`** (730 days): Changing to this retention sets `retention_expiration_date = cleanup_status_date + 730 days`

- **`1095d`** (1095 days): Changing to this retention sets `retention_expiration_date = cleanup_status_date + 1095 days`

Example: when the retention of a simulation of thousands of simulation is change to 90d in the VSM-UI then we sets the `retention_expiration_date = cleanup_status_date + 90 days`


### Retention State Calculation

The retention state can be calculated from the following parameters:

- **`path`**: This is the main retention parameter. ALL simulations under the path must be classified as path protected, even if there is nothing to clean or the simulation has issues such as reproducibility, missing, or multiple ".set" files

- **`clean`**: If the simulation was cleaned by the system or other tools, then set the retention to clean

- **`issue`**: If the simulation cannot be cleaned due to `.set` - file issues or lack of reproducibility (depending on LaC' requirements)

### Calculation Logic

The calculation of the retention for the remaining simulations is based on: 
`days_to_cleanup = retention_expiration_date - cleanup_status_date`

**Retention State Ranges:**
- **`marked`**: `days_to_cleanup ≤ cleanup_frequency`
- **`next`**: `cleanup_frequency < days_to_cleanup ≤ 2 × cleanup_frequency`
- **`90d`**: `2 × cleanup_frequency < days_to_cleanup ≤ 90 days`
- **`180d`**: `90 < days_to_cleanup ≤ 180 days`
- **`365d`**: `180 < days_to_cleanup ≤ 365 days`
- **`730d`**: `365 < days_to_cleanup ≤ 730 days`
- **`1095d`**: `730 < days_to_cleanup` 


## Retention Label Assignment Algorithm

The following pseudocode demonstrates how to determine the appropriate retention label for a simulation based on its calculated days to cleanup:

```pseudocode
// Input: the_simulations_days_to_cleanup (calculated days until cleanup)
// Output: cat_retention_label (assigned retention category)

i = -1
cat_days_to_cleanup = retention_catalog.values  // [cleanup_frequency, 2*cleanup_frequency, 90, 180, 365, 730, 1095]

while (++i < len(cat_days_to_cleanup) && cat_days_to_cleanup[i] <= the_simulations_days_to_cleanup) {
    // Continue until we find the appropriate category
}

cat_retention_label = retention_catalog[i].key
```

### Algorithm Explanation

This algorithm iterates through the retention catalog values in ascending order to find the first category where the simulation's days to cleanup exceeds the category threshold. The simulation is then assigned to that retention category.

**Example:**
- If a simulation has 80 days to cleanup, it gets assigned the "90d" retention label:
- If a simulation has 45 days to cleanup and cleanup_frequency is 30 days:
  - It exceeds 30 (cleanup_frequency) but is ≤ 60 (2 × cleanup_frequency)
  - Therefore, it gets assigned the "next" retention label
