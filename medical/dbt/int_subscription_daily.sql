{{ config(materialized='table') }}

WITH int_subscriptions AS (
    SELECT * FROM {{ ref('int_subscriptions') }}
)

, dim_dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
)

, int_pause_daily AS (
    SELECT * FROM {{ ref('int_pause_daily') }}
)

, int_pause_timeline AS (
    SELECT * FROM {{ ref('int_pause_timeline') }}
)

, int_cancel_daily AS (
    SELECT * FROM {{ ref('int_cancel_daily') }}
)

, int_billing_issue_daily AS (
    SELECT * FROM {{ ref('int_billing_issue_daily') }}
)

, int_renewal_timeline AS (
    SELECT * FROM {{ ref('int_renewal_timeline') }}
)

, int_order_history AS (
    SELECT * FROM {{ ref('int_order_history') }}
)

, starting_point AS (
    SELECT MIN(start_date_pst) AS starting_date FROM int_subscriptions
)

-- Daily Active Users (DAU) with Churn Rate
, date_spine AS (
  -- Create a row for each day from earliest subscription to current date
    SELECT
        d.date AS date_day
        , DATE(CURRENT_TIMESTAMP(), 'America/Los_Angeles') AS current_date_pst
    FROM dim_dates d
    CROSS JOIN starting_point s
    WHERE d.date BETWEEN s.starting_date AND DATE(CURRENT_TIMESTAMP(), 'America/Los_Angeles')
)

, subscription_daily AS (
    SELECT
        ds.date_day
        , ds.current_date_pst
        , fs.uuid
        , fs.admin_user_id
        , fs.protocol
        , fs.subscription_sequence
        , fs.next_subscription_sequence
        , fs.start_date
        , fs.start_date_pst
        , fs.next_start_date
        , fs.next_start_date_pst
        , ioh.admin_email
        , ioh.product_category_2
        , ioh.mm_plan
        , irt.active_mm_plan
        , COALESCE(
            irt.active_mm_plan
            , ioh.mm_plan
            , 1
        ) AS subscription_length
        , COALESCE(
            icd.cancellation_created_at
            , icdf.future_cancel_created_at
        ) AS subscription_cancelled
        , COALESCE(
            icd.cancelled_at
            , icdf.future_cancel_scheduled_for
        ) AS intended_cancellation_date
        , COALESCE(
            icd.cancelled_at_dt_pst
            , icdf.future_cancel_scheduled_for_dt_pst
        ) AS intended_cancellation_dt_pst
        , COALESCE(
            icd.cancelled_at_pst
            , icdf.cancelled_at_pst
        ) AS intended_cancellation_pst
        , COALESCE(
            icd.is_autocancel
            , icdf.future_cancel_is_autocancel
        ) AS is_autocancel
        , COALESCE(
            icd.system_cancellation_reason
            , icdf.future_cancel_system_cancellation_reason
        ) AS system_cancellation_reason
        , ioh.order_transaction_date AS last_transaction_date
        , ioh.order_transaction_dt AS last_transaction_dt
        , ioh.billing_cycle_days
        , ioh.is_bnpl
        , ioh.is_bnpl_fod
        , ioh.contains_bb
        , ioh.contains_appt
        , COALESCE(
            icdf.future_cancel_scheduled_for_dt_pst IS NOT NULL
            AND ds.date_day < icdf.future_cancel_scheduled_for_dt_pst
            , FALSE
        ) AS has_future_intended_cancellation
        , ipd.pause_at
        , ipd.resume_at
        , COALESCE(ipd.admin_user_id IS NOT NULL, FALSE) AS is_paused
        , COALESCE(
            COALESCE(
                icd.admin_user_id
                , icdf.admin_user_id
            ) IS NOT NULL
            , FALSE
        ) AS is_cancelled
        , COALESCE(
            icd.admin_user_id IS NOT NULL
            , FALSE
        ) AS is_cancelled_today
        , COALESCE(ibd.admin_user_id IS NOT NULL, FALSE) AS has_billing_issue
        -- Replicate fct_subscriptions is_active logic for point-in-time:
        -- A subscription is inactive if past 18 months or past renewal + 5 day grace.
        -- Paused subs that are not cancelled are exempt (always active).
        -- Uses point-in-time last_transaction_date from int_order_history (ioh)
        -- and point-in-time active_mm_plan from int_renewal_timeline (irt).
        , COALESCE(
            (
                irt.active_mm_plan IS NOT NULL
                AND DATE_DIFF(ds.date_day, ioh.order_transaction_dt, DAY)
                <= irt.active_plan_days
            )
                -- Fallback to original subscription length checks
            OR (
                ioh.mm_plan = 3
                AND DATE_DIFF(ds.date_day, ioh.order_transaction_dt, DAY) <= 90
            )
            OR (
                ioh.mm_plan = 12
                AND DATE_DIFF(ds.date_day, ioh.order_transaction_dt, DAY) <= 365
            )
            OR (
                ioh.mm_plan NOT IN (3, 12)
                AND DATE_DIFF(ds.date_day, ioh.order_transaction_dt, DAY)
                <= COALESCE(ioh.mm_plan, 1) * 30
            )
            , FALSE
        ) AS is_active_past_renewal
        , COALESCE(
            ioh.order_transaction_dt IS NOT NULL
            AND ioh.order_transaction_dt < DATE_SUB(ds.date_day, INTERVAL 18 MONTH)
            , FALSE
        ) AS has_old_transaction
        , CASE
            -- When it has either a current or future cancellation, no renewal/end date
            WHEN COALESCE(icd.admin_user_id, icdf.admin_user_id) IS NOT NULL
            THEN NULL
            -- ADD: Semaglutide Starter Pack override FIRST
            WHEN ioh.product_category_2 = 'Weight Loss Semaglutide Starter Pack'
                AND ioh.order_transaction_dt > '2025-06-10'
            THEN TIMESTAMP_ADD(ioh.order_transaction_dt, INTERVAL 105 DAY)
            -- THEN existing logic
            WHEN ioh.is_bnpl = 'Yes' THEN
                TIMESTAMP_ADD(ioh.order_transaction_dt, INTERVAL 1 MONTH)
            -- PRIORITIZE: Users with plan changes should use their new active plan length
            WHEN irt.active_mm_plan IS NOT NULL THEN
                TIMESTAMP_ADD(
                    ioh.order_transaction_dt
                    , INTERVAL irt.active_mm_plan MONTH
                )
            -- THEN: billing_cycle_days for users without active plans
            WHEN ioh.billing_cycle_days IS NOT NULL THEN
                TIMESTAMP_ADD(ioh.order_transaction_dt, INTERVAL ioh.billing_cycle_days DAY)
            -- Fallback to original subscription_length
            ELSE
                TIMESTAMP_ADD(
                    ioh.order_transaction_dt
                    , INTERVAL COALESCE(ioh.mm_plan, 1) MONTH
                )
        END AS renewal_or_end_date
        , COALESCE(
            DATE_DIFF(ds.date_day, ioh.order_transaction_dt, DAY) <= 365
            , FALSE
        ) AS has_transaction_within_past_year
        , CASE
            WHEN ibd.admin_user_id IS NOT NULL
            THEN ibd.billing_issue_from
        END AS billing_issue_unresolved
        , CASE
            -- If subscription has a cancellation still in effect
            WHEN icd.admin_user_id IS NOT NULL
                AND icd.cancelled_at_dt_pst <= ds.date_day
                THEN
                    -- Calculate from start to cancellation date
                    ROUND(
                        TIMESTAMP_DIFF(
                            icd.cancelled_at_dt_pst
                            , fs.start_date_pst
                            , DAY
                        ) / 30.0
                        , 2
                    )
            -- MODIFIED: Only consider billing issue unresolved if no successful payment after issue
            WHEN
                ibd.admin_user_id IS NOT NULL
                THEN
                -- Calculate from start to billing issue date
                ROUND(
                    DATE_DIFF(
                        ibd.billing_issue_from_date_pst
                        , fs.start_date_pst
                        , DAY
                    ) / 30.0
                    , 2
                )
            -- If subscription is still active
            ELSE
                -- Calculate from start to today
                ROUND(
                    DATE_DIFF(
                        ds.date_day
                        , fs.start_date_pst
                        , DAY
                    ) / 30.0
                    , 2
                )
        END AS tenure_months
    FROM date_spine ds
    JOIN int_subscriptions fs
    -- Subscription started before or on this day
    ON fs.start_date_pst <= ds.date_day
        -- Inclusive of all days up until the day
        -- before a new subscription. Subscriptions
        -- stop "updating" with new information
        -- on their last day (or today, if they're
        -- current.)
        AND (
            ds.date_day < fs.next_start_date_pst
            OR fs.next_start_date_pst IS NULL
        )
    LEFT JOIN int_pause_daily ipd
    ON fs.admin_user_id = ipd.admin_user_id
        AND fs.protocol = ipd.protocol
        AND ds.date_day = ipd.date_day
        AND ipd.pause_created_at_dt_pst >= fs.start_date_pst
        AND (
            ipd.pause_created_at_dt_pst < fs.next_start_date_pst
            OR fs.next_start_date_pst IS NULL
        )
    LEFT JOIN int_cancel_daily icd
    ON fs.admin_user_id = icd.admin_user_id
        AND fs.protocol = icd.protocol
        AND ds.date_day = icd.day_date
        -- Only joins in instances where the subscription is cancelled
        -- on the date being evaluated.
        AND icd.actively_cancelled
        AND icd.cancelled_at_dt_pst >= fs.start_date_pst
        AND (
            icd.cancelled_at_dt_pst < fs.next_start_date_pst
            OR fs.next_start_date_pst IS NULL
        )
    LEFT JOIN int_cancel_daily icdf
    ON fs.admin_user_id = icdf.admin_user_id
        AND fs.protocol = icdf.protocol
        AND ds.date_day = icdf.day_date
        -- Only joins in instances where the subscription is scheduled
        -- to be cancelled but not yet cancelled.
        AND icdf.cancelled_in_future
        AND icdf.future_cancel_created_dt_pst >= fs.start_date_pst
        AND (
            icdf.future_cancel_created_dt_pst < fs.next_start_date_pst
            OR fs.next_start_date_pst IS NULL
        )
    LEFT JOIN int_billing_issue_daily ibd
    ON fs.admin_user_id = ibd.admin_user_id
        AND ds.date_day = ibd.date_day
        AND ibd.billing_issue_from_date_pst >= fs.start_date_pst
        AND (
            ibd.billing_issue_from_date_pst < fs.next_start_date_pst
            OR fs.next_start_date_pst IS NULL
        )
    LEFT JOIN int_renewal_timeline irt
    ON fs.admin_user_id = irt.admin_user_id
        AND fs.protocol = irt.protocol
        AND ds.date_day = irt.date_day
        AND irt.created_at_dt_pst >= fs.start_date_pst
        AND (
            irt.created_at_dt_pst < fs.next_start_date_pst
            OR fs.next_start_date_pst IS NULL
        )
        -- The table that the joining model replaces doesn't yet do either
        -- of the protocols filtered out. The condition below should be
        -- commented out in a future PR.
        AND irt.protocol NOT IN ('Growth Hormone', 'Mood, Stress & Sleep')
    LEFT JOIN int_order_history ioh
    ON fs.admin_user_id = ioh.admin_user_id
        AND fs.protocol = ioh.protocol
        AND fs.subscription_sequence = ioh.subscription_sequence
        AND ds.date_day = ioh.date_day
        AND ioh.order_transaction_dt >= fs.start_date_pst
        AND (
            ioh.order_transaction_dt < fs.next_start_date_pst
            OR fs.next_start_date_pst IS NULL
        )
)

, day_before_flags AS (
    SELECT
        s.*
        , COALESCE(
            s.is_cancelled_today AND COALESCE(db.is_cancelled_today, FALSE) = FALSE
            , FALSE
        ) AS cancelled_today
        , COALESCE(
            s.is_paused AND COALESCE(db.is_paused, FALSE) = FALSE
            , FALSE
        ) AS paused_today
        , COALESCE(
            s.renewal_or_end_date IS NOT NULL
             AND s.date_day > DATE_ADD(DATE(s.renewal_or_end_date), INTERVAL 5 DAY)
             , FALSE
        ) AS is_past_renewal_date
        , CASE WHEN s.active_mm_plan IS NOT NULL
            AND s.active_mm_plan != s.subscription_length
            THEN TRUE
            ELSE FALSE
        END AS has_plan_change
        , COALESCE(
            pp.paused_at IS NOT NULL
            , FALSE
        ) AS past_pause
        , pp.paused_date_pst AS past_pause_at
        , pp.resumed_date_pst AS past_resume_at
        , COALESCE(
            fp.paused_at IS NOT NULL
            , FALSE
        ) AS future_pause
        , fp.paused_date_pst AS future_pause_at
        , fp.resumed_date_pst AS future_resume_at
        , CASE WHEN s.is_cancelled THEN 'voluntary'
            WHEN s.has_billing_issue
            AND s.billing_issue_unresolved > s.last_transaction_date
            THEN 'billing_issue'
        END AS cancellation_reason
        , COALESCE(db.uuid IS NULL AND s.uuid IS NOT NULL, FALSE) AS is_new_subscription
        , ROW_NUMBER() OVER (
            PARTITION BY s.admin_user_id
            ORDER BY s.date_day, s.protocol, s.start_date_pst
        ) = 1 AS is_new_user
        , ROW_NUMBER() OVER (
            PARTITION BY s.admin_user_id, s.protocol
            ORDER BY s.date_day, s.start_date_pst
        ) = 1 AS is_new_protocol_user
    FROM subscription_daily s
    LEFT JOIN subscription_daily db
    ON s.uuid = db.uuid
        AND s.date_day = DATE_ADD(db.date_day, INTERVAL 1 DAY)
    LEFT JOIN int_pause_timeline pp
    ON s.admin_user_id = pp.admin_user_id
    AND s.protocol = pp.protocol
    -- Only joins in a record if the subscription was
    -- paused on/after the day the subscription started
    -- and resumed before the subscription ended
    -- (or today if the subscription never ended)
    AND pp.paused_date_pst >= s.start_date_pst
    AND pp.resumed_date_pst < COALESCE(
        s.next_start_date_pst
        , DATE_ADD(s.date_day, INTERVAL 1 DAY)
    )
    -- Limits join to only pauses we "know about" on a given
    -- day -- those created on or before the day of the record
    AND pp.created_dt_pst <= s.date_day
    LEFT JOIN int_pause_timeline fp
    ON s.admin_user_id = fp.admin_user_id
    -- Only joins in a record if the subscription has a
    -- future pause scheduled for sometime after today
    -- that will either resume sometime after today or
    -- doesn't have a resume timestamp.
    AND s.protocol = fp.protocol
    AND fp.paused_date_pst > s.date_day
    AND (
        fp.resumed_date_pst > s.date_day
        OR fp.resumed_date_pst IS NULL
    )
    -- Same limiter -- only pauses we "know about"
    AND fp.created_dt_pst <= s.date_day
    -- The joins to historical pauses and future pauses
    -- can both cause duplicates, in which case
    -- we want the past pause closest to the day
    -- of the record, or the one immediately following
    -- the day of the record
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            s.date_day
            , s.admin_user_id
            , s.protocol
            , s.subscription_sequence
        ORDER BY
            pp.paused_at DESC
            , fp.paused_at ASC
    ) = 1
)

, add_active_flag AS (
    SELECT
        *
        , COALESCE(
            CASE
                WHEN is_paused AND NOT is_cancelled_today THEN TRUE
                WHEN has_future_intended_cancellation THEN TRUE
                WHEN has_old_transaction THEN FALSE
                WHEN has_billing_issue THEN FALSE
                WHEN is_past_renewal_date THEN FALSE
                ELSE NOT is_cancelled
                    AND has_transaction_within_past_year
            END
            , FALSE
        ) AS is_active
    FROM day_before_flags
)

, semifinal AS (
    SELECT
        t.*
        , MAX(
            CASE WHEN t.is_new_user THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                t.admin_user_id, DATE_TRUNC(t.date_day, WEEK (MONDAY))
            ORDER BY t.date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS is_new_user_this_week
        , MAX(
            CASE WHEN t.is_new_protocol_user THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                t.admin_user_id
                , t.protocol
                , DATE_TRUNC(t.date_day, WEEK (MONDAY))
            ORDER BY t.date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS is_new_protocol_user_this_week
        , MAX(
            CASE WHEN t.is_new_user THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                t.admin_user_id, DATE_TRUNC(t.date_day, MONTH)
            ORDER BY t.date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS is_new_user_this_month
        , MAX(
            CASE WHEN t.is_new_protocol_user THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                t.admin_user_id
                , t.protocol
                , DATE_TRUNC(t.date_day, MONTH)
            ORDER BY t.date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS is_new_protocol_user_this_month
        , COALESCE(
            -- User || protocol is active today
            -- and wasn't active yesterday. Covers instances where
            -- user fell out of billing issue, etc., as well as
            -- cancellations that were uncancelled.)
            dbp.is_active = FALSE AND t.is_active
            , FALSE
        ) AS reactivated_today
        , COALESCE(
            -- User || protocol is BNPL today
            -- and wasn't BNPL yesterday
            dbp.is_bnpl = 'No' AND t.is_bnpl = 'Yes'
            , FALSE
        ) AS became_bnpl_today
        , COALESCE(
            -- User || protocol is not BNPL today
            -- and wasn BNPL yesterday
            dbp.is_bnpl = 'Yes' AND t.is_bnpl = 'No'
            , FALSE
        ) AS exited_bnpl_today
    FROM add_active_flag t
    LEFT JOIN add_active_flag dbp
    ON t.admin_user_id = dbp.admin_user_id
        AND t.protocol = dbp.protocol
        AND t.date_day = DATE_ADD(dbp.date_day, INTERVAL 1 DAY)
)

, final AS (
    SELECT
        TO_HEX(
            MD5(
                CONCAT(
                    COALESCE(CAST(date_day AS STRING), '')
                    , COALESCE(uuid, '')
                )
            )
        ) AS uuid
        , date_day
        , uuid AS subscription_uuid
        , admin_user_id
        , admin_email
        , protocol
        , CONCAT(
            COALESCE(admin_user_id, '')
            , COALESCE(protocol, '')
        ) AS user_protocol
        , CASE
            WHEN product_category_2 IN (
                'Testosterone E/P'
                , 'Testosterone Enclo'
                , 'Testosterone Enclo+'
            )
            THEN 'Enclomiphene'
            ELSE protocol END
        AS protocol_expanded
        , product_category_2
        , subscription_sequence
        , next_subscription_sequence
        , start_date
        , DATE(start_date, 'America/Los_Angeles') AS start_dt_pst
        , DATETIME(start_date, 'America/Los_Angeles') AS start_date_pst
        , next_start_date
        , next_start_date_pst
        , active_mm_plan
        , mm_plan
        , billing_cycle_days
        , subscription_length
        , SAFE_CAST(subscription_length AS INT64) AS subscription_length_int
        , tenure_months
        , last_transaction_date
        , last_transaction_dt
        , billing_issue_unresolved
        , renewal_or_end_date
        , subscription_cancelled
        , cancellation_reason
        , system_cancellation_reason
        , intended_cancellation_date
        , intended_cancellation_pst
        , intended_cancellation_dt_pst
        , has_future_intended_cancellation
        , pause_at
        , resume_at
        , past_pause
        , past_pause_at
        , past_resume_at
        , future_pause
        , future_pause_at
        , future_resume_at
        , is_autocancel
        , is_active
        , is_paused
        , is_cancelled
        , is_cancelled_today
        , has_billing_issue
        , is_active_past_renewal
        , has_plan_change
        , has_old_transaction
        , has_transaction_within_past_year
        , is_past_renewal_date
        , paused_today
        , cancelled_today
        , reactivated_today
        , is_bnpl
        , is_bnpl_fod
        , contains_bb
        , contains_appt
        , is_new_subscription
        , is_new_user
        , is_new_protocol_user
        , is_new_user_this_week
        , is_new_protocol_user_this_week
        , is_new_user_this_month
        , is_new_protocol_user_this_month
        , became_bnpl_today
        , exited_bnpl_today
        , MAX(
            CASE WHEN reactivated_today THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                admin_user_id, DATE_TRUNC(date_day, WEEK (MONDAY))
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS reactivated_user_this_week
        , MAX(
            CASE WHEN reactivated_today THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                admin_user_id
                , protocol
                , DATE_TRUNC(date_day, WEEK (MONDAY))
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS reactivated_sub_this_week
        , MAX(
            CASE WHEN reactivated_today THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                admin_user_id, DATE_TRUNC(date_day, MONTH)
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS reactivated_user_this_month
        , MAX(
            CASE WHEN reactivated_today THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                admin_user_id
                , protocol
                , DATE_TRUNC(date_day, MONTH)
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS reactivated_sub_this_month
        -- became bnpl weekly
        , MAX(
            CASE WHEN became_bnpl_today THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                admin_user_id, DATE_TRUNC(date_day, WEEK (MONDAY))
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS became_bnpl_user_this_week
        , MAX(
            CASE WHEN became_bnpl_today THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                admin_user_id
                , protocol
                , DATE_TRUNC(date_day, WEEK (MONDAY))
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS became_bnpl_sub_this_week
        -- exited bnpl weekly
        , MAX(
            CASE WHEN exited_bnpl_today THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                admin_user_id, DATE_TRUNC(date_day, WEEK (MONDAY))
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS exited_bnpl_user_this_week
        , MAX(
            CASE WHEN exited_bnpl_today THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                admin_user_id
                , protocol
                , DATE_TRUNC(date_day, WEEK (MONDAY))
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS exited_bnpl_sub_this_week
        -- became bnpl monthly
        , MAX(
            CASE WHEN became_bnpl_today THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                admin_user_id, DATE_TRUNC(date_day, MONTH)
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS became_bnpl_user_this_month
        , MAX(
            CASE WHEN became_bnpl_today THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                admin_user_id
                , protocol
                , DATE_TRUNC(date_day, MONTH)
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS became_bnpl_sub_this_month
        -- exited bnpl monthly
        , MAX(
            CASE WHEN exited_bnpl_today THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                admin_user_id, DATE_TRUNC(date_day, MONTH)
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS exited_bnpl_user_this_month
        , MAX(
            CASE WHEN exited_bnpl_today THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                admin_user_id
                , protocol
                , DATE_TRUNC(date_day, MONTH)
            ORDER BY date_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) = 1 AS exited_bnpl_sub_this_month
        , DATE_DIFF(
            next_start_date_pst, start_date_pst, DAY
        ) AS switch_lag
        , current_date_pst
    FROM semifinal
)

SELECT * FROM final
