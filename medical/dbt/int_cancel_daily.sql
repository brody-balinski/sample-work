WITH cancelled_subscriptions AS (
    SELECT
        *
        , DATE(CURRENT_TIMESTAMP(), 'America/Los_Angeles') AS current_date_pst
    FROM {{ ref('int_cancellation_timeline') }}
)

, dim_dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
)

, dim_product_name AS (
    SELECT * FROM {{ ref('dim_product_name') }}
)

, cancel_created_expansion AS (
    SELECT
        d.date AS day_date
        , p.admin_user_id
        , p.product_name
        , p.cancellation_created_at
        , p.system_cancellation_reason
        , p.cancelled_at
        , p.uncancelled_at
        , p.cancelled_at_dt_pst
        , p.uncancelled_at_dt_pst
        , p.cancellation_created_at_dt_pst
        , CASE WHEN p.system_cancellation_reason IN (
            'Inactive System Cancel'
            , '30+ days of billing issue'
            )
            THEN 1 ELSE 0 END
        AS is_autocancel
    FROM dim_dates d
    JOIN cancelled_subscriptions p
    ON d.date >= p.cancellation_created_at_dt_pst
        -- If cancellation is no longer pending (implied by the date
        -- reaching cancelled_at), we don't want a record for it --
        -- the cancellation has then actually gone into affect.
        AND d.date < p.cancelled_at_dt_pst
        AND (
            d.date < p.next_cancellation_created_at_dt_pst
            OR p.next_cancellation_created_at_dt_pst IS NULL
        )
)

, final AS (
    SELECT
        COALESCE(d.date, cce.day_date) AS day_date
        , COALESCE(p.admin_user_id, cce.admin_user_id) AS admin_user_id
        , COALESCE(p.product_name, cce.product_name) AS product_name
        , dpn.protocol
        , p.cancellation_created_at
        , p.system_cancellation_reason
        , p.cancelled_at
        , p.uncancelled_at
        , p.cancelled_at_pst
        , p.cancelled_at_dt_pst
        , p.uncancelled_at_dt_pst
        , p.cancellation_created_at_dt_pst
        , CASE WHEN p.system_cancellation_reason IN (
            'Inactive System Cancel'
            , '30+ days of billing issue'
            )
            THEN 1 ELSE 0 END
        AS is_autocancel
        , cce.cancellation_created_at AS future_cancel_created_at
        , DATE(cce.cancellation_created_at, 'America/Los_Angeles') AS future_cancel_created_dt_pst
        , cce.cancelled_at AS future_cancel_scheduled_for
        , cce.uncancelled_at AS future_cancel_scheduled_to_end
        , cce.system_cancellation_reason AS future_cancel_system_cancellation_reason
        , DATE(cce.cancelled_at, 'America/Los_Angeles') AS future_cancel_scheduled_for_dt_pst
        , COALESCE(p.cancelled_at IS NOT NULL, FALSE) AS actively_cancelled
        , COALESCE(cce.cancelled_at IS NOT NULL, FALSE) AS cancelled_in_future
        , cce.is_autocancel AS future_cancel_is_autocancel
    FROM dim_dates d
    JOIN cancelled_subscriptions p
    -- If there's no uncancelled_at date, the subscription is still
    -- cancelled; thus we can extend its respective cancellation
    -- record through the current day.
    ON d.date >= p.cancelled_at_dt_pst
    AND (
        d.date < p.uncancelled_at_dt_pst
        OR p.uncancelled_at_dt_pst IS NULL
    )
    FULL OUTER JOIN cancel_created_expansion cce
    ON d.date = cce.day_date
        AND p.admin_user_id = cce.admin_user_id
        AND p.product_name = cce.product_name
    LEFT JOIN dim_product_name dpn
    ON COALESCE(p.product_name, cce.product_name) = dpn.product_name
)

SELECT * FROM final
ORDER BY
  admin_user_id
  , protocol
  , cancelled_at
  , day_date
