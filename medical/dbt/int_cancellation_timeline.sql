WITH stg_adminprod_events AS (
    SELECT * FROM {{ ref('stg_adminprod_events') }}
)

, dim_product_name AS (
    SELECT * FROM {{ ref('dim_product_name') }}
)

, cancellation_events AS (
    SELECT
        *
        , LAG(event_type) OVER (
            PARTITION BY admin_user_id, product_name
            ORDER BY created_at, version
        ) AS previous_event_type
    FROM stg_adminprod_events
    WHERE event_type IN (
        'Values::Events::AccountCancelled'
        , 'Values::Events::AccountUncancelled'
    )
    AND product_name IS NOT NULL
)

, group_events AS (
    SELECT
        *
        , SUM(
            CASE WHEN event_type != previous_event_type
            THEN 1 ELSE 0 END
            ) OVER (
                PARTITION BY admin_user_id, product_name
                ORDER BY created_at, version
        ) AS nbr_group
    FROM cancellation_events
)

, numbered AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY admin_user_id, product_name, nbr_group
            -- When two consecutve events occur (they shouldn't -- a cancelled
            -- event should always precede an uncancelled event), if an
            -- uncancelled event or cancelled_at is missing, we're assuming that
            -- the most recent event contains the most accurate information
            -- (sometimes we see this when a user cancels for a future date, then adjusts
            -- that future date, triggering another cancel event.)
            -- Dan also did some manual cancelling that would obviously trump out the
            -- AccountCancelled event preceding it.
            ORDER BY
                created_at DESC
                , version
        ) AS consecutive_event_nbr
    FROM group_events
)

, filter_out_consecutives AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY admin_user_id, product_name
            ORDER BY created_at, version
        ) AS sort_nbr
    FROM numbered
    WHERE consecutive_event_nbr = 1
)

, account_cancelled_timeline AS (
    -- Now that Cancelled and Uncancelled events have been
    -- deduped, we can safely join them to one another.
    -- A Cancelled event will always precede an
    -- Uncancelled event, so we can rely on sequence
    -- for a join condition.
    SELECT
        c.admin_user_id
        , c.product_name
        , c.created_at
        , c.cancelled_at
        , c.operator_type
        , c.operator_id
        , c.system_cancellation_reason
        , c.cancel_immediately
        , c1.created_at AS uncancelled_at
        , LAG(c1.created_at) OVER (
            PARTITION BY c.admin_user_id, c.product_name
            ORDER BY c.created_at, c.version
        ) AS lag_uncancelled_at
        , ROW_NUMBER() OVER (
            PARTITION BY c.admin_user_id, c.product_name
            ORDER BY c.created_at, c.version
        ) AS new_sort_nbr
    FROM filter_out_consecutives c
    LEFT JOIN filter_out_consecutives c1
    ON c.admin_user_id = c1.admin_user_id
        AND c.product_name = c1.product_name
        AND c.sort_nbr = c1.sort_nbr - 1
        AND c1.event_type = 'Values::Events::AccountUncancelled'
    WHERE c.event_type = 'Values::Events::AccountCancelled'
        -- Filters out instances where a future cancellation is
        -- uncancelled before it can kick in.
        AND (c.cancelled_at < c1.created_at
            OR c1.created_at IS NULL
            OR c.cancelled_at IS NULL
        )
)

, semifinal AS (
    SELECT
        t.admin_user_id
        , t.product_name
        , dpn.protocol
        , t.created_at AS cancellation_created_at
        , DATE(t.created_at, 'America/Los_Angeles') AS cancellation_created_at_dt_pst
        , LEAD(t.created_at) OVER (
            PARTITION BY t.admin_user_id, t.product_name
            ORDER BY t.created_at
        ) AS next_cancellation_created_at
        , t.operator_type
        , t.operator_id
        , t.system_cancellation_reason
        , t.cancel_immediately
        , COALESCE(
            t.lag_uncancelled_at
            , CASE
                WHEN t.new_sort_nbr = 1
                THEN CAST('1900-01-01 00:00:00' AS TIMESTAMP)
                END
        ) AS valid_from
        , COALESCE(
            -- cancelled_at isn't always populated, and sometimes
            -- it's incorrectly capturing the cancelled_at of a
            -- previous cancel
            CASE
                WHEN t.cancelled_at > t.lag_uncancelled_at
                OR (
                    t.lag_uncancelled_at IS NULL
                    AND t.new_sort_nbr = 1
                    AND t.cancelled_at IS NOT NULL
                )
                THEN t.cancelled_at
            END
            , t.created_at
        ) AS cancelled_at
        , t.uncancelled_at
    FROM account_cancelled_timeline t
    LEFT JOIN dim_product_name dpn
    ON t.product_name = dpn.product_name
)

, final AS (
    SELECT
        admin_user_id
        , product_name
        , protocol
        , cancellation_created_at
        , cancellation_created_at_dt_pst
        , next_cancellation_created_at
        , DATE(
            next_cancellation_created_at, 'America/Los_Angeles'
        ) AS next_cancellation_created_at_dt_pst
        , operator_type
        , operator_id
        , system_cancellation_reason
        , cancel_immediately
        , valid_from
        , cancelled_at
        , uncancelled_at
        , DATE(cancelled_at) AS cancelled_at_dt_utc
        , DATE(uncancelled_at) AS uncancelled_at_dt_utc
        , DATE(cancelled_at, 'America/Los_Angeles') AS cancelled_at_dt_pst
        , DATE(uncancelled_at, 'America/Los_Angeles') AS uncancelled_at_dt_pst
        , DATETIME(cancelled_at, 'America/Los_Angeles') AS cancelled_at_pst
    FROM semifinal
)

SELECT * FROM final
