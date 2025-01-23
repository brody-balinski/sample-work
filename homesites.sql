/* 
Source dataset: A daily webscrape of a competitor's national homesite inventory, offering us insights into sales, pricing
behaviors, and geographical availability

Objectives:
	1.) Clean the data
	2.) Parse any necessary fields
	3.) Add any flags and fields needed for downstream reporting

Aggregation: No
*/

WITH homesites AS (
    SELECT * FROM ml_homesites
)

, community_daily AS (
    SELECT * FROM ml_community_daily
)

, map_community AS (
    SELECT * FROM ml_map_community
)

, map_plan AS (
    SELECT * FROM ml_map_plan
)

-- Gives us the most recent information for every community, as determined by report_date
, community_latest AS (
    SELECT * FROM community_daily
    QUALIFY ROW_NUMBER() OVER (PARTITION BY community_id ORDER BY report_date DESC) = 1
)

, homesites_clean AS (
    SELECT
        *
        -- The price column is sometimes non-numerical and instead contains a status. In all non-numerical occurrences,
        -- the status column is null. As such, whenever the status column is null, we want to check the price column
        -- for a non-numerical value (a status.) Outside of status, we only want to capture the price value if it is
        -- numerical.
        -- Regarding run_date, with the timing of our scraping job, we've decided to lag the day by one.
        REPLACE(
            COALESCE(status, IFF(NOT REGEXP_LIKE(REPLACE(price, '.', ''), '^[0-9]+$'), price, NULL)) AS status
            , TRY_CAST(price AS FLOAT) AS price
            , DATEADD(DAY, -1, run_date) AS run_date
            , REPLACE(
                REPLACE(
                    INITCAP(floor_plan)
                    , ' Ii'
                    , ' II'
                ), ' ||'
                , ' II'
            ) AS floor_plan
            , REPLACE(city_address, 'Saint ', 'St. ') AS city_address
        )
        RENAME (
            run_date AS report_date
        )
        -- geo extraction logic
        -- city
        , INITCAP(
            CASE WHEN LENGTH(SUBSTR(city_address, POSITION(',' IN city_address) + 1)) != 6
                THEN SUBSTR(city_address, 1, POSITION(',' IN city_address) - 1)
                ELSE TRIM(REGEXP_SUBSTR(city_address, '^(.*) [A-Z]{2},', 1, 1, 'e'))
            END
            , ' ')
            AS city
        -- state
        , CASE WHEN LENGTH(SUBSTR(city_address, POSITION(',' IN city_address) + 1)) = 6
            THEN SUBSTR(city_address, POSITION(',' IN city_address) - 2, 2)
            ELSE SUBSTR(city_address, POSITION(', ' IN city_address) + 2, 2) END
            AS state
        -- zip code
        , REGEXP_SUBSTR(SUBSTR(city_address, POSITION(',' IN city_address) + 1), '\\d+$') AS zip_code
        -- home trait extraction logic
        -- bed. Sometimes (rarely) changes. Assumption is that the most recent value is the most
        -- acurate.
        , LAST_VALUE(TRY_TO_DECIMAL(house_info['bed']::STRING, 6, 1)) OVER(
            PARTITION BY homesite_id ORDER BY run_date
        ) AS bed
        -- bath. Sometimes (rarely) changes. Assumption is that the most recent value is the most
        -- acurate.
        , LAST_VALUE(TRY_TO_DECIMAL(house_info['bath']::STRING, 6, 1)) OVER(
            PARTITION BY homesite_id ORDER BY run_date
        ) AS bath
        -- garage
        , LAST_VALUE(TRY_TO_DECIMAL(house_info['garage']::STRING, 6, 1)) OVER(
            PARTITION BY homesite_id ORDER BY run_date
        ) AS garage
        -- story
        , LAST_VALUE(TRY_TO_NUMBER(house_info['story']::STRING)) OVER(
            PARTITION BY homesite_id ORDER BY run_date
        ) AS story
        -- sqft
        , LAST_VALUE(TRY_TO_NUMBER(house_info['sqft']::STRING)) OVER(
            PARTITION BY homesite_id ORDER BY run_date
        ) AS sqft
        -- date logic
        -- captures the earliest recorded report_date per homesite_id where the homesite was available
        , MIN(CASE WHEN status NOT IN ('Under Contract', 'Closed') THEN report_date END) OVER (
            PARTITION BY homesite_id ORDER BY report_date
        ) AS first_seen_avail_dt
        -- captures the latest recorded report_date per homesite_id where the homesite was available
        , MAX(CASE WHEN status NOT IN ('Under Contract', 'Closed') THEN report_date END) OVER (
            PARTITION BY homesite_id ORDER BY report_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_seen_avail_dt
        , MD5(
            report_date || homesite_id
        ) AS daily_homesite_key
        , LAG(report_date) OVER (PARTITION BY homesite_id ORDER BY report_date) AS previous_report_date
        , MIN(report_date) OVER () AS min_report_dt
    FROM homesites
    -- Data before 10/14 is unreliable. We limit it to 10/13 (represented by a 10/14 report_date per the replace /
	-- rename logic above) so that we can capture any sales that may have happened on 10/14 via a status change.
    WHERE report_date >= '2024-10-14'
    -- homesite_id is a hash of address, community name and company name. A homesite should only appear once on
    -- Any given day. Exact duplicates sometimes appear on the website, requiring us to filter them out.
    QUALIFY ROW_NUMBER() OVER(PARTITION BY homesite_id, report_date ORDER BY prefect_uploaded_at DESC) = 1
)

-- floor plans and community ids can sometimes change for the same homesite id. We've created external mapping tables
-- that standardize floor plans across homesites that are the same plan, and community ids across homesites belonging
-- to the same community
, homesites_clean_proper_dims AS (
    SELECT
        hc.*
        REPLACE(
            mc.rpt_community_id AS community_id
            , mc.rpt_community_name AS community_name
            , mp.rpt_floor_plan AS floor_plan
        )
    FROM homesites_clean hc
    LEFT JOIN map_community mc
        ON hc.community_id = mc.community_id
    LEFT JOIN map_plan mp
        ON mc.rpt_community_id = mp.community_id
        AND hc.floor_plan = mp.floor_plan
        AND hc.bed = mp.bed
        AND hc.bath = mp.bath
)

, parsed AS (
    SELECT
        cd.*
        -- There are occurrences where a homesite moves from being present but not having a status to having a
        -- status originally located in the price field (thus our numerical price field is null.) In these
        -- instances, we still want to capture the price of the sale while recognizing that it occurred on the
        -- day of the status change, so we bring in the numerical price value from the day before.
        REPLACE(
            CASE WHEN db.homesite_id IS NOT NULL AND COALESCE(db.status, '') = '' AND cd.status = 'Under Contract'
                THEN db.price ELSE cd.price
            END AS price
        )
        , c.area_name
        , c.first_seen_avail_dt AS comm_first_seen_avail_dt
        , db.price AS price_previous_report_dt
        , db.status AS status_previous_report_dt
        , cd.price - db.price AS price_chg_dod
        -- The typical progression of a homesite in the sales funnel is that it goes from "Now Selling" to "Under Contract"
        -- and then "Under Contract" to "Closed" or "Sold." As such, we've created two flags, one for each progression
        -- scenario. For a sale, we want to see a homesite that is newly seeing an "Under Contract" status, which includes
        -- homesites that may have a null status the day before. However, homesites that didn't have a record the day before
        -- also may have a null status the day before, so we check that the join to records from the day before is
        -- successful via the "db.homesite_id IS NOT NULL" clause.
        , COALESCE(
            db.status NOT IN ('Under Contract', 'Closed')
            AND cd.status = 'Under Contract'
            AND db.homesite_id IS NOT NULL, FALSE)
            AS sale_flag
        , COALESCE(
            db.status IN ('Under Contract', 'Closed')
            AND cd.status = 'Now Selling', FALSE)
            AS cancel_flag
        -- earliest recorded numerical price value
        , FIRST_VALUE(
            cd.price IGNORE NULLS) OVER (
            PARTITION BY cd.homesite_id
            ORDER BY cd.report_date
        ) AS earliest_price
        -- We capture the latest sale date for our days_since_first_listed count, which we limit after the point of a sale
        -- if one has occurred because homesites are often listed past their sale date. We don't want to say a homesite
        -- has been listed for 12 days if for 8 of them it was already sold, for instance.
        , MAX(CASE WHEN sale_flag = TRUE THEN cd.report_date END) OVER(
            PARTITION BY cd.homesite_id ORDER BY cd.report_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS latest_sale_dt
        -- We capture the latest cancellation for calculating days on market. If a homesite has been sold, cancelled, and is on
        -- the market again, we want to calculate the difference between when it was re-introduced to the market and its latest
        -- sale date rather than when it first was introduced to the market.
        , MAX(CASE WHEN cancel_flag = TRUE THEN cd.report_date END) OVER(
            PARTITION BY cd.homesite_id ORDER BY cd.report_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS latest_cancel_dt
        -- calculates the difference in days between when the homesite first appeared in the table as available and
        -- the report_date being evaluated. If the home has been sold, days since first listed is frozen at the
        -- difference between when it first appeared and day of the sale
        , CASE WHEN cd.status IN ('Under Contract', 'Closed')
            THEN DATEDIFF(DAY, cd.first_seen_avail_dt, latest_sale_dt)
            ELSE DATEDIFF(DAY, cd.first_seen_avail_dt, cd.report_date) END
            AS days_since_first_listed
        -- flags the record if it's the first appearance of a homesite in the table (according to report_date)
        , COALESCE(cd.first_seen_avail_dt = cd.report_date, FALSE) AS first_seen_avail_flag
        -- flags the record if it's the final appearance of a homesite in the table (according to report_date)
        , COALESCE(cd.last_seen_avail_dt = cd.report_date, FALSE) AS last_seen_avail_flag
    FROM homesites_clean_proper_dims cd -- noqa:ST09
    LEFT JOIN community_latest c
        ON c.community_id = cd.community_id
    -- LEFT JOIN to the same table ensures that we captures all records for the run date being evaluated.
    -- The join condition of the previous report date being equal to the report date ensures that we bring in
    -- records from the immediate last record (sorted by report date.)
    LEFT JOIN homesites_clean_proper_dims db
        ON cd.homesite_id = db.homesite_id
        AND cd.previous_report_date = db.report_date
)

, days_on_market_added AS (
    SELECT
        *
        , COUNT(CASE WHEN sale_flag = TRUE THEN latest_sale_dt END) OVER (
            PARTITION BY homesite_id ORDER BY report_date ASC
        ) AS sale_count
        , COUNT(CASE WHEN cancel_flag = TRUE THEN latest_cancel_dt END) OVER (
            PARTITION BY homesite_id ORDER BY report_date ASC
        ) AS cancel_count
        -- Calculates the number of days that a homesite has been on the website as available for purchase.
        -- 1.) If a homesite appears the same day as the community first appeared in our initial scrape of the website, we
        -- don't want to consider it in our days on market calculations, so we default it to NULL.
        -- 2.) If a homesite has been sold but not cancelled, it's calculated as the difference between the day a homesite
        -- was first seen and the most recent sale date
        -- 3.) If a homesite has been cancelled once or more and has sold once or more and has a current status of "Under
        -- Contract" or "Closed", it's calculated as the difference between the most recent cancellation date and the most
        -- recent sale date
        -- 4.)  If a homesite has been cancelled once or more and sold once or more and has a current status of "Now
        -- Selling", it's calculated as the difference between the most recent cancellation date and the report_date
        -- 5.) In all other instances (namely no sales or cancellations), it's calculated as the difference between the
        -- day a homesite was first seen and the report_date
        , CASE
            WHEN first_seen_avail_dt = comm_first_seen_avail_dt AND comm_first_seen_avail_dt = min_report_dt THEN NULL
            WHEN cancel_count = 0 AND sale_count > 0
            THEN DATEDIFF(DAY, first_seen_avail_dt, latest_sale_dt)
            WHEN cancel_count > 0 AND sale_count > 0 AND status IN ('Under Contract', 'Closed')
            THEN DATEDIFF(DAY, latest_cancel_dt, latest_sale_dt)
            WHEN cancel_count > 0 AND sale_count > 0 AND status NOT IN ('Under Contract', 'Closed')
            THEN DATEDIFF(DAY, latest_cancel_dt, report_date)
            ELSE DATEDIFF(DAY, first_seen_avail_dt, report_date) END
            AS days_on_market
    FROM parsed
)

, final AS (
    SELECT
        daily_homesite_key
        , report_date
        , company
        , area_name
        , homesite_id
        , community_id
        , community_name
        , street_address
        , city
        , state
        , zip_code
        , floor_plan
        , bed
        , bath
        , garage
        , sqft
        , story
        , price
        , price_previous_report_dt
        , price_chg_dod
        , earliest_price
        , status
        , status_previous_report_dt
        , days_since_first_listed
        , days_on_market
        , first_seen_avail_dt
        , last_seen_avail_dt
        , latest_sale_dt
        , latest_cancel_dt
        , first_seen_avail_flag
        , last_seen_avail_flag
        , sale_flag
        , cancel_flag
        , prefect_uploaded_at
    FROM days_on_market_added
)

SELECT * FROM final
;
