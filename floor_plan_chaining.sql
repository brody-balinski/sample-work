
/*
Objective: Being that floor_plan names of homesites sometimes change when the homesite itself hasn't
actually changed plans (its square footage, # bedrooms, and # bathrooms has stayed the same),
we'd like to determine which floor_plan values belong to the same plan. This will also help us
identify when two or more homesites belong to the same plan (which becomes important in downstream
aggregations and so forth.) After determining which floor_plan values belong to the same plan, we
then assign all homesites belonging to it with the floor_plan value that first appeared.


Example:
	homesites table
	--
	REPORT_DATE		HOMESITE		COMMUNITY		FLOOR_PLAN
	----------------------------------------------------------------------------------
	2024-01-02		5			ABC			1
	2024-01-03		5			ABC			2
	2024-01-04		5			ABC			3
	2024-01-03		6			ABC			2
	2024-01-04		6			ABC			3
	2024-01-04		6			ABC			4
	
Both homesites in the example above belong to the same community and have floor_plan values "2" and "3" in
common, indicating to us that they truly belong to the same plan. The progression through floor_plan values
can be illustrated as follows:

	HOMESITE		FLOOR_PLAN_LIST
	----------------------------------------
	5			[1, 2, 3]
	6			[2, 3, 4]
	
Our objective is to first chain the two lists together to create one comprehensive floor_plan list, then to
assign the first value in the list to every homesite with a floor_plan value in the comprehensive list:

	FULL FLOOR_PLAN LIST:
	[1, 2, 3, 4]
	
	Revised homesites table records:

	REPORT_DATE		HOMESITE		COMMUNITY		FLOOR_PLAN
	-------------------------------------------------------------------------------------------
	2024-01-02		5			ABC			1
	2024-01-03		5			ABC			1
	2024-01-04		5			ABC			1
	2024-01-03		6			ABC			1
	2024-01-04		6			ABC			1
	2024-01-04		6			ABC			1
*/

WITH homesites AS (
    SELECT * FROM homesites
)

-- Tells us when each floor_plan first appeared for each homesite_id
, ranked_floor_plans AS (
    SELECT
        homesite_id
        , community_id
        , floor_plan
        , MIN(bed) AS bed
        , MIN(bath) AS bath
        , MIN(report_date) AS first_seen_date
        , MAX(report_date) AS last_seen_date
    FROM homesites
    GROUP BY 1, 2, 3
)

-- Gives us a distinct list of all available community || floor plan || bed || bath combinations
, dtx_community_plans AS (
    SELECT DISTINCT
        community_id
        , floor_plan
        , bed
        , bath
    FROM ranked_floor_plans
)

-- Produces an array of floor plans belonging to the same homesite, ordered by when they were
-- first seen
, floor_plan_list AS (
    SELECT
        homesite_id
        , community_id
        -- At the homesite_id level, bed / bath won't change, so taking the min of each gives us the
        -- value of each
        , MIN(bed) AS bed
        , MIN(bath) AS bath
        , ARRAY_AGG(floor_plan) WITHIN GROUP (ORDER BY first_seen_date) AS floor_plans
        , MIN(first_seen_date) AS first_seen_date
        , MAX(last_seen_date) AS last_seen_date
    FROM ranked_floor_plans
    GROUP BY 1, 2
)

-- Produces a list of all possible floor_plan combinations. When two or more homesites see the
-- same combination of floor plans, we filter for the combination showing the most recent appearance.
, floor_plan_list_filtered AS (
    SELECT
        community_id
        , floor_plans
        , bed
        , bath
        , first_seen_date
        , last_seen_date
    -- homesites can progress through the same floor_plan values. This filters out duplicate
    -- progressions, giving us a distinct list of all possible progressions (prioritized by
    -- which one has the most recent record.)
    FROM floor_plan_list
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY community_id, floor_plans, bed, bath
        ORDER BY last_seen_date DESC
    ) = 1
)

-- Recursive CTE that iteratively chains together lists of floor_plan values to form one list of all
-- possible floor_plan values (rpt_floor_plan) belonging to the same community. Limited to five iterations,
-- as we noticed that there's typically only a maximum of 3 segments of community ids that overlap each other
-- but wanted to include a buffer. Only chains together sequential community_ids -- those that come after one
-- another in order of when the final floor_plan in a list was last seen.
, matches AS (
    SELECT
        f.community_id
        , f.floor_plans
        , f.floor_plans AS rpt_floor_plans
        , f.bed
        , f.bath
        , 0 AS match_cnt
        , f.first_seen_date
        , f.last_seen_date
        , f.last_seen_date AS final_seen_date
        , 0 AS iteration
    FROM floor_plan_list_filtered f

    UNION ALL

    SELECT
        m.community_id
        , m.floor_plans
        , ARRAY_DISTINCT(
            ARRAY_CAT(
                m.rpt_floor_plans
                , COALESCE(
                    f1.floor_plans
                    , ARRAY_CONSTRUCT()
                )
            )
        ) AS rpt_floor_plans
        , m.bed
        , m.bath
        , m.match_cnt + CASE WHEN f1.floor_plans IS NOT NULL THEN 1 ELSE 0 END AS match_cnt
        , m.first_seen_date
        , COALESCE(
            f1.last_seen_date
            , m.last_seen_date
        ) AS last_seen_date
        , COALESCE(
            f1.last_seen_date
            , m.last_seen_date
        ) AS final_seen_date
        , m.iteration + 1 AS iteration
    FROM matches m
    LEFT JOIN floor_plan_list_filtered f1
        -- Lists of plans are from the same community
        ON m.community_id = f1.community_id
        -- Plan lists are for the same number of bedrooms
        AND m.bed = f1.bed
        -- Plan lists are for the same number of bathrooms
        AND m.bath = f1.bath
        -- lists of plans have at least one floor_plan in common
        AND ARRAYS_OVERLAP(m.floor_plans, f1.floor_plans)
        -- the subset of the communities that the two lists have in common is the beginning of the second list
        AND ARRAY_INTERSECTION(m.floor_plans, f1.floor_plans)
        = ARRAY_SLICE(f1.floor_plans, 0, ARRAY_SIZE(ARRAY_INTERSECTION(m.floor_plans, f1.floor_plans)))
        -- the lists of floor plans are not identical
        AND m.floor_plans != f1.floor_plans
        -- the joining list has more than one floor plan. We aren't interested in bringing in a list via join if
        -- it isn't going to point us in the direction of another floor plan
        AND ARRAY_SIZE(f1.floor_plans) > 1
        -- the latest value of the joining array comes after the latest value of the base array
        AND m.last_seen_date < f1.last_seen_date
    WHERE iteration < 5
)

-- We're only interested in the final iteration of the recursive CTE -- where the longest possible list exists
, matches_clean AS (
    SELECT
        *
        , MAX(ARRAY_SIZE(rpt_floor_plans)) OVER(
            PARTITION BY community_id, bed, bath, rpt_floor_plans[ARRAY_SIZE(rpt_floor_plans) - 1]
        ) AS max_list_size
    FROM matches
    WHERE iteration = 5
)

-- Creates a list of all possible floor_plan combinations when there is more than one possible floor_plan for a homesite
, matches_filtered AS (
    SELECT
        *
        -- We want the first condition to evaluate to null if there wasn't any matches between the initial floor_plan list and others,
        -- so that if a homesite sees the total number of floor plans in its first list, it can be considered for the QUALIFY
        -- statement below as being the row selected.
        , COALESCE(
            CASE WHEN match_cnt > 0 THEN match_cnt END
            , CASE WHEN ARRAY_SIZE(floor_plans) = max_list_size
                THEN 2
            END
        ) AS match_score
        -- Creates a unique identifier for each floor plan list per community
        , ROW_NUMBER() OVER (PARTITION BY community_id, bed, bath ORDER BY first_seen_date)::STRING AS list_id
    FROM matches_clean
    -- Before OR: Filters out single floor plan instances, as we're only interested in lists with multiple floor_plan values for
    -- this CTE. However, single-community instances can match to themselves, so we include the additional condition of rpt_floor_plans
    -- (which is a distinct array) holding more than one value.
    -- After OR: Even if there aren't any matches to an initial floor plan list, we still want to use it, being that it may be the entire
    -- chain of floor plans. If the initial floor plan list had two or more floor plans but doesn't yield any matches, it's the entire
    -- list of floor plan progressions, so we include it.
    WHERE (match_score > 0 AND ARRAY_SIZE(rpt_floor_plans) > 1)
        OR (ARRAY_SIZE(floor_plans) > 1 AND match_cnt = 0)
    -- Joins in the recursive CTE sometimes result in the same list of floor plans, but we only need one record of a list, so we filter
    -- out duplicates
    QUALIFY ROW_NUMBER() OVER (PARTITION BY community_id, bed, bath, rpt_floor_plans ORDER BY first_seen_date) = 1
)




-- A few number of lists aren't sequential, where homesites within the community cycle through floor plans, each with a changing
-- floor_plan value at a different time. These require their own recursive CTE, where sequence isn't a condition of the join.
, matches_semifinal AS (
    SELECT
        mf.community_id
        , mf.rpt_floor_plans
        , mf.rpt_floor_plans AS rpt_floor_plans_full
        , mf.bed
        , mf.bath
        , mf.first_seen_date
        , ARRAY_CONSTRUCT(mf.list_id) AS list_of_lists
        , 0 AS iter
    FROM matches_filtered mf

    UNION ALL

    SELECT
        ms.community_id
        , ms.rpt_floor_plans
        , ARRAY_DISTINCT(
            ARRAY_CAT(
                ms.rpt_floor_plans_full
                , COALESCE(
                    mf1.rpt_floor_plans
                    , ARRAY_CONSTRUCT()
                )
            )
        ) AS rpt_floor_plans_full
        , ms.bed
        , ms.bath
        , ms.first_seen_date
        -- Since we're combining lists, we need to keep track of which lists we've already added to each list. If join conditions
        -- are met, this updates an array with a list of the lists that have already been added to the base list.
        , CASE WHEN mf1.list_id IS NOT NULL THEN ARRAY_APPEND(ms.list_of_lists, mf1.list_id)
            ELSE ms.list_of_lists
        END AS list_of_lists
        , ms.iter + 1 AS iter
    FROM matches_semifinal ms
    LEFT JOIN matches_filtered mf1
        -- Lists stem from the same community
        ON ms.community_id = mf1.community_id
        -- Plan lists are for the same number of bedrooms
        AND ms.bed = mf1.bed
        -- Plan lists are for the same number of bathrooms
        AND ms.bath = mf1.bath
        -- Lists have at least one floor_plan in common
        AND ARRAYS_OVERLAP(ms.rpt_floor_plans_full, mf1.rpt_floor_plans)
        -- If we're joining to a list, we want to make sure it's a list that we haven't previously joined to
        AND NOT ARRAY_CONTAINS(mf1.list_id::VARIANT, ms.list_of_lists)
    -- We limit the number of iterations to 5
    WHERE iter < 5
)

-- Some of the joins in the previous CTE can result in the same lists. We filter those out with the QUALIFY statement, opting to
-- preserve the list that has the earliest dated first community_id (being that the first community_id later becomes the one we
-- use in reporting.)
, matches_final AS (
    SELECT *
    FROM matches_semifinal
    WHERE iter = 5
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY community_id, bed, bath, ARRAY_SORT(rpt_floor_plans_full)
        ORDER BY first_seen_date ASC
    ) = 1
)

, semifinal AS (
    SELECT -- noqa:ST06
        d.community_id
        , d.floor_plan
        , d.bed
        , d.bath
        , COALESCE(
            m.rpt_floor_plans_full[0]::STRING
            , d.floor_plan
        ) AS rpt_floor_plan
    FROM dtx_community_plans d
    LEFT JOIN matches_final m
        ON d.community_id = m.community_id
        AND d.bed = m.bed
        AND d.bath = m.bath
        AND ARRAY_CONTAINS(d.floor_plan::VARIANT, m.rpt_floor_plans_full)
)

, final AS (
	SELECT
		h.* REPLACE(
			sf.rpt_floor_plan AS floor_plan
		)
	FROM homesites h
	LEFT JOIN semifinal sf
	ON h.community_id = sf.community_id
		AND h.floor_plan = sf.floor_plan
		AND h.bed = sf.bed
		AND h.bath = sf.bath
)

SELECT * FROM final
;
