/*
Objective: To determine the number of consecutive weeks that a healthcare product has been assigned its respective
availability status at the record level.

The inventory table lists healthcare products (that exist within distribution centers) per week; each of which has a status
that tells us whether the product is "In Stock," "Low Stock," or "Out of Stock." Products sometimes aren't recorded
due to being temporarily removed from our offerings, so weekly appearance isn't always sequential. A product's status can
also change over time.

inventory
---------
DISTRIBUTION_CENTER   PRODUCT_ID      REPORT_WEEK     AVAILABILITY_STATUS
-------------------------------------------------------------------------
CENT001               441562         2024-04-29      In Stock
CENT001               441562         2024-05-06      In Stock
CENT001               441562         2024-05-20      In Stock
CENT001               441562         2024-05-27      Low Stock
CENT001               441562         2024-06-03      Low Stock
CENT001               441562         2024-06-10      Out of Stock
CENT001               441562         2024-06-17      Out of Stock
CENT001               441562         2024-06-24      In Stock

*/

WITH inventory AS (
    SELECT * FROM inventory
)

-- Step 1: Identify the availability status of the same product in its preceding record (sorted by report week),
-- and determine if the preceding record belongs to the immediate previous week. If the product's status today is
-- different from the previous record or if the preceding record doesn't belong to the immediate previous week, we flag it.
, window_weeks AS (
    SELECT 
        *,
        LAG(report_week) OVER (
            PARTITION BY distribution_center, product_id ORDER BY report_week
        ) AS previous_report_week,
        LAG(availability_status) OVER (
            PARTITION BY distribution_center, product_id ORDER BY report_week
        ) AS previous_availability_status,
        CASE 
            WHEN report_week != DATEADD('week', 1, previous_report_week) THEN 1 
        END AS has_date_gap_flag,
        CASE 
            WHEN availability_status != previous_availability_status THEN 1 
        END AS has_different_status_flag
    FROM inventory
)

/*
Looking at a sample product, 441562, it isn't reported for the week of 2024-05-13, raising the
has_date_gap_flag. Additionally, it transitions from "Low Stock" to "Out of Stock" on 2024-06-10, raising the
has_different_status_flag.

inventory
---------
DISTRIBUTION_CENTER   PRODUCT_ID      REPORT_WEEK     AVAILABILITY_STATUS	HAS_DATE_GAP_FLAG	HAS_DIFFERENT_STATUS_FLAG
-------------------------------------------------------------------------------------------------------------------------
CENT001               441562         2024-04-29      In Stock				
CENT001               441562         2024-05-06      In Stock				
CENT001               441562         2024-05-20      In Stock				1
CENT001               441562         2024-05-27      Low Stock						1
CENT001               441562         2024-06-03      Low Stock
CENT001               441562         2024-06-10      Out of Stock					1
CENT001               441562         2024-06-17      Out of Stock
CENT001               441562         2024-06-24      In Stock						1
*/


-- Step 2: Create rolling sums of the flags. week_grp sums the has_date_gap_flag, increasing when a report week is skipped.
-- availability_status_grp sums the has_different_status_flag, increasing when the availability status changes.
, week_grouping AS (
    SELECT
        *,
        SUM(has_date_gap_flag) OVER (
            PARTITION BY distribution_center, product_id ORDER BY report_week
        ) AS week_grp,
        SUM(has_different_status_flag) OVER (
            PARTITION BY distribution_center, product_id ORDER BY report_week
        ) AS availability_status_grp
    FROM window_weeks
)

/*
Looking at the same sample product, 441562, week_grp increases whenever there is a gap in report weeks (as flagged by the 
has_date_gap_flag field.) Similarly, availability_status_grp increases whenever the availability status changes (as flagged
by the has_different_status_flag field.)

DISTRIBUTION_CENTER   PRODUCT_ID   REPORT_WEEK     HAS_DATE_   AVAILABILITY_   HAS_DIFFERENT_   WEEK_GRP   AVAILABILITY_
						   GAP_FLAG    STATUS	        STATUS_FLAG		   STATUS_GRP
-------------------------------------------------------------------------------------------------------------------------
CENT001               441562      2024-04-29                   In Stock                         1          1
CENT001               441562      2024-05-06                   In Stock                         1          1
CENT001               441562      2024-05-20      1            In Stock                         2          1
CENT001               441562      2024-05-27                   Low Stock       1      		2          2
CENT001               441562      2024-06-03                   Low Stock              		2          2
CENT001               441562      2024-06-10                   Out of Stock    1      		2          3
CENT001               441562      2024-06-17                   Out of Stock           		2          3
CENT001               441562      2024-06-24                   In Stock        1      		2          4
*/

-- Step 3: Assign row numbers by segmenting based on both the availability status grouping and the week grouping.
-- This gives us the number of consecutive weeks that a product has been in its availability status for its respective
-- report week. If the status changes or if a week is skipped, the row count starts again at 1.
, final AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY distribution_center, product_id, availability_status_grp, week_grp 
            ORDER BY report_week
        ) AS consecutive_weeks_in_status
    FROM week_grouping
)

SELECT * FROM final;

/*
Final Output:
This table shows how the flags, groupings, and consecutive week calculations work together to form the consecutive_weeks_in_status
field.

DISTRIBUTION   PRODUCT_ID   REPORT_WEEK     HAS_DATE_    AVAILABILITY_   HAS_DIFFERENT_   WEEK_GRP   AVAILABILITY_  CONSECUTIVE
_CENTER					    _GAP_FLAG	 STATUS		 _STATUS_FLAG		     STATUS_GRP	    _WEEKS_IN_STATUS
------------------------------------------------------------------------------------------------------------------------------------
CENT001        PROD123      2024-04-29                   In Stock                         1          1		    1
CENT001        PROD123      2024-05-06                   In Stock                         1          1		    2
CENT001        PROD123      2024-05-20      1            In Stock                         2          1              1
CENT001        PROD123      2024-05-27                   Low Stock      1                 2          2	 	    1
CENT001        PROD123      2024-06-03                   Low Stock                        2          2              2
CENT001        PROD123      2024-06-10                   Out of Stock   1                 2          3              1
CENT001        PROD123      2024-06-17                   Out of Stock                     2          3              2
CENT001        PROD123      2024-06-24                   In Stock       1                 2          4              1
*/
