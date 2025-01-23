
# Overview
This repository contains SQL scripts designed to address various analytical and operational challenges I've come across while working. Each script provides detailed solutions for specific use cases, with a focus on flexibility, reusability, and performance.

**Contents**
### 1. consecutive_weeks_in_status
   #### &nbsp;&nbsp;&nbsp;&nbsp;Description:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Calculates the number of consecutive weeks that a healthcare product has maintained a particular inventory status.
   #### &nbsp;&nbsp;&nbsp;&nbsp;Key Features:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Tracks status transitions and reporting gaps. Identifies patterns of consistency or change. Outputs consecutive week counts
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;segmented by entity and status group.
   #### &nbsp;&nbsp;&nbsp;&nbsp;Use Case:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Ideal for monitoring the number of consecutive weeks that something has maintained the same description.
### 2. floor_plan_chaining
   #### &nbsp;&nbsp;&nbsp;&nbsp;Description:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Uses recursive joins to chain together floor plans so that homesites belonging to the same floor plan can be identified as
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;such.
   #### &nbsp;&nbsp;&nbsp;&nbsp;Key Features:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Provides a mapping table for a homesite's correct floor plan (used in the homesites script)
   #### &nbsp;&nbsp;&nbsp;&nbsp;Use Case:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Useful for any scenario where multiple items may belong to the same descriptor, but that descriptor's value can change over time.
### 3. homesites
   #### &nbsp;&nbsp;&nbsp;&nbsp;Description:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Focuses on cleaning and transforming webscraped homesites data
   #### &nbsp;&nbsp;&nbsp;&nbsp;Key Features:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Handles complex joins, parsing logic, and window functions of various types.
   #### &nbsp;&nbsp;&nbsp;&nbsp;Use Case:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Logic is typical of that you may see in the earlier layer of a data pipeline; before aggregation occurs.
