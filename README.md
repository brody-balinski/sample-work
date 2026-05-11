
# Overview
This repository contains SQL scripts designed to address various analytical and operational challenges I've come across while working. Each script provides detailed solutions for specific use cases, with a focus on flexibility, reusability, and performance.

# Contents

## Homebuilding
### dbt
### 1. consecutive_weeks_in_status
   #### &nbsp;&nbsp;&nbsp;&nbsp;Description:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Calculates the number of consecutive weeks that a healthcare product has maintained a<br>
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;particular inventory status.
   #### &nbsp;&nbsp;&nbsp;&nbsp;Key Features:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Tracks status transitions and reporting gaps. Identifies patterns of consistency or change.<br>
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Outputs consecutive week counts segmented by entity and status group.
   #### &nbsp;&nbsp;&nbsp;&nbsp;Use Case:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Ideal for monitoring the number of consecutive weeks that something has maintained the<br>
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;same description.
### 2. floor_plan_chaining
   #### &nbsp;&nbsp;&nbsp;&nbsp;Description:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Uses recursive joins to chain together floor plans so that homesites belonging to the same floor plan<br>
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;can be identified as such.
   #### &nbsp;&nbsp;&nbsp;&nbsp;Key Features:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Provides a mapping table for a homesite's correct floor plan (used in the homesites script)
   #### &nbsp;&nbsp;&nbsp;&nbsp;Use Case:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Useful for any scenario where multiple items may belong to the same descriptor, but that descriptor's<br>
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;value can change over time.
### 3. homesites
   #### &nbsp;&nbsp;&nbsp;&nbsp;Description:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Focuses on cleaning and transforming webscraped homesites data
   #### &nbsp;&nbsp;&nbsp;&nbsp;Key Features:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Handles complex joins, parsing logic, and window functions of various types.
   #### &nbsp;&nbsp;&nbsp;&nbsp;Use Case:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Logic is typical of that you may see in the earlier layer of a data pipeline; before aggregation occurs.

## Medical
### python
### 1. ingestion_medication_requests
   #### &nbsp;&nbsp;&nbsp;&nbsp;Description:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Orchestrates the ingestion of medication request data from an API, using an incrementality strategy<br>
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;based on when the record was last updated in the API.
### 2. survey_response_scoring
   #### &nbsp;&nbsp;&nbsp;&nbsp;Description:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Uses an LLM to score the freeform survey responses from customers in a series of pre-defined buckets.<br>
### dbt
### 1. int_cancellation_timeline
   #### &nbsp;&nbsp;&nbsp;&nbsp;Description:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Creates a timeline table of when cancellations occurred, and for how long they were in effect, for a<br>
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;specific user and protocol (since one user can have multiple protocols.)
### 2. int_cancel_daily
   #### &nbsp;&nbsp;&nbsp;&nbsp;Description:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Expands the aforementioned cancellations timeline table out into a daily table, so that on any given<br>
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;day, we can answer the question of whether a user || protocol combination was cancelled or not.)
### 3. int_subscription_daily
   #### &nbsp;&nbsp;&nbsp;&nbsp;Description:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Uses the daily cancellations table, along with other tables that were expanded in similar fashion, to<br>
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;create a daily status table, giving us daily details about every subscription, past and present.
### 4. stg_medication_requests
   #### &nbsp;&nbsp;&nbsp;&nbsp;Description:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Staging table for the data brought into the database by the medication_request_ingestion script.<br>
### 5. int_prescriptions
   #### &nbsp;&nbsp;&nbsp;&nbsp;Description:
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Intermediary table for the medication request data. Joins in qualitative fields. Sources from<br>
   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;stg_medication_requests.
