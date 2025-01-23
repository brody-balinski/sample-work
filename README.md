
This repository contains SQL scripts designed to address various analytical and operational challenges I've come across while working. Each script provides detailed solutions for specific use cases, with a focus on flexibility, reusability, and performance.

**Contents**
###1. consecutive_weeks_in_status
   Description: Calculates the number of consecutive weeks that a healthcare product has maintained a particular inventory status.
   Key Features: Tracks status transitions and reporting gaps. Identifies patterns of consistency or change. Outputs consecutive week counts segmented by entity and status group.
   Use Case: Ideal for monitoring the number of consecutive weeks that something has maintained the same description.
2. floor_plan_chaining
   Description: Uses recursive joins to chain together floor plans so that homesites belonging to the same floor plan can be identified as such.
   Key Features: Provides a mapping table for a homesite's correct floor plan (used in the homesites script)
   Use Case: Useful for any scenario where multiple items may belong to the same descriptor, but that descriptor's value can change over time.
4. homesites
   Description: Focuses on cleaning and transforming webscraped homesites data
   Key Features: Handles complex joins, parsing logic, and window functions of various types.
   Use Case: Logic is typical of that you may see in the earlier layer of a data pipeline; before aggregation occurs.
