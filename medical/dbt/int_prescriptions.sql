/*
    Enriches Medplum prescriptions data with shipping information
*/

{{ config(materialized='table') }}


WITH stg_medication_requests AS (
    SELECT * FROM {{ ref('stg_medication_requests') }}
)

, int_prescriptions_shipments AS (
    SELECT * FROM {{ ref('int_prescriptions_shipments') }}
)

, stg_adminprod_prescriptions_shipments AS (
    SELECT * FROM {{ ref('stg_adminprod_prescriptions_shipments') }}
)

, map_pharmacy_medplum_admin AS (
    SELECT * FROM {{ ref('map_pharmacy_medplum_admin') }}
)

, deleted_prescriptions AS (
    SELECT * FROM {{ ref('deleted_prescriptions') }}
)

, dim_customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
)

, stg_medication_dispense AS (
    SELECT * FROM {{ ref('stg_medication_dispense') }}
)

, pharmacy_shipment_data AS (
    SELECT * FROM {{ ref('stg_pharmacy_shipment_data') }}
    WHERE provider = '<censored>'
)

, prescriptions_shipments_first AS (
    SELECT
        foreign_rx_number
        , prescription_status_timestamp
        , tracking_number
        , shipment_carrier
        -- The same foreign_rx_number will never have more than one distinct status
        , prescription_status
    FROM stg_adminprod_prescriptions_shipments
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY foreign_rx_number
        ORDER BY prescription_status_timestamp
    ) = 1
)

, first_medication_dispense AS (
    -- Filters stg_medication_dispense for the first shipment
    -- of each prescription
    SELECT * FROM stg_medication_dispense
    WHERE refill_nbr = 0
)

, joined AS (
    SELECT
        p.* EXCEPT (
            admin_prescription_id
            , days_supply
            , pharmacy_name_raw
            , pharmacy_name
            , is_cancelled
        )
        , COALESCE(
            p.admin_prescription_id
            , ms.admin_prescription_id
            , ms2.admin_prescription_id
        ) AS admin_prescription_id
        , COALESCE(ms.intake_name, ms2.intake_name, ms3.intake_name) AS intake_name
        , COALESCE(
            p.days_supply
            , ms.days_supply
            , ms2.days_supply
            , ms3.days_supply
            , md.days_supply
        ) AS days_supply
        , COALESCE(
            ea.admin_user_id
            , ms.admin_user_id
            , ms2.admin_user_id
            , ms3.admin_user_id
        ) AS admin_user_id
        -- Prioritizes the pharmacy name of the first shipment of a medication request -- if
        -- it exists -- otherwise defaults to the pharmacy associated with the medication
        -- request itself.
        , COALESCE(md.pharmacy_name_raw, p.pharmacy_name_raw, 'Other') AS pharmacy_name_raw
        , COALESCE(md.pharmacy_name, p.pharmacy_name) AS pharmacy_name
        , {{ prescription_medication_category('medication_name', 'p') }} AS medication_category
        , COALESCE(
            -- September 2025 and onward, we see more non-00:00:00 timestamps from the
            -- MedicationDispense resource. This applies the same timestamp logic as above if
            -- the date_shipped is 00:00:00 (which, according to the data, doesn't ever
            -- happen naturally.)
            CASE
                WHEN md.date_shipped_is_simple
                THEN TIMESTAMP(
                    DATETIME(DATE(md.date_shipped), TIME(12, 0, 0))
                    , 'America/Los_Angeles'
                )
                ELSE md.date_shipped
            END
            , TIMESTAMP(b.prescription_status_timestamp)
            , bp.shipment_ts
            -- Since the shipment date from admin comes in as a date, we need to convert it
            -- to a timestamp, but can't simply wrap it in a TIMESTAMP() since midnight UTC
            -- is 4 PM the previous day. As such, we convert it to 12:00 PST, which is safely
            -- within the same day in EST and in UTC (matching the date it first showed as.)
            , TIMESTAMP(
                DATETIME(DATE(ms.date_shipped), TIME(12, 0, 0))
                , 'America/Los_Angeles'
            )
            , TIMESTAMP(
                DATETIME(DATE(ms2.date_shipped), TIME(12, 0, 0))
                , 'America/Los_Angeles'
            )
            , TIMESTAMP(
                DATETIME(DATE(ms3.date_shipped), TIME(12, 0, 0))
                , 'America/Los_Angeles'
            )
        ) AS shipment_date
        , COALESCE(
            b.tracking_number
            , bp.tracking_number
            , ms.tracking_number
            , ms2.tracking_number
            , ms3.tracking_number
        ) AS tracking_number
        , COALESCE(
            b.shipment_carrier
            , bp.shipment_carrier
            , ms.carrier
            , ms2.carrier
            , ms3.carrier
        ) AS shipment_carrier
        , COALESCE(
            b.prescription_status_timestamp
            , bp.shipment_ts
        ) AS <pharmacy_censored>_shipment_date
        -- Per Daniel, pharmacy name data is to be sourced from Medication Requests for Medplum
        , '' AS family_everwell_shipment_date
        , '' AS tailormade_shipment_date
        , '' AS precision_shipment_date
        , COALESCE(
            ms.date_shipped
            , ms2.date_shipped
            , ms3.date_shipped
        ) AS admin_shipment_date
        , COALESCE(
            b.prescription_status_timestamp
            , bp.shipment_ts
            , ms.shpmt_created_at
            , ms2.shpmt_created_at
            , ms3.shpmt_created_at
            , md.api_last_updated
        ) AS shpmt_updated
        , COALESCE(
            b.prescription_status
            , bp.prescription_status
        ) AS <pharmacy_censored>_prescription_status
        , ms.operator_type
        -- Field from MedicationDispense began populating with the delivery
        -- date on 2026-04-08. 
        , CASE WHEN md.api_last_updated_dt_pst >= '2026-04-08'
            THEN md.delivered_at END
        AS delivered_at
        , 0 AS has_shipment_info
        , COALESCE(
            p.pharmacy_name = 'Other'
            , false
        ) AS is_atypical_pharmacy
        , COALESCE(
            p.medication_name IN (
                '<censored_1>'
                , '<censored_2>'
                , '<censored_3>'
            )
            , false
        ) AS contains_bb
        , d.is_deleted
        , COALESCE(
            p.is_cancelled OR d.is_deleted IS NOT null
            , false
        ) AS is_cancelled
    FROM stg_medication_requests p
    LEFT JOIN int_prescriptions_shipments ms
    -- Join condition specifies that we capture the first shipment
    -- of the prescription with ms.refill_nbr = 0. If there are no
    -- refills for the prescription, refill_nbr will still be 0.
    ON p.external_prescription_id = ms.external_prescription_id
        AND ms.refill_nbr = 0
        AND ms.admin_with_shipping_nbr = 1
    -- Same comment applies to the join below; we're just using the
    -- <pharmacy_legacy_censored> identifier to bring the data in
    LEFT JOIN int_prescriptions_shipments ms2
    ON p.<pharmacy_legacy_censored>_prescription_id = ms2.external_prescription_id
        AND ms2.refill_nbr = 0
        AND ms2.admin_with_shipping_nbr = 1
    -- Same comment applies to the join below; we're just using the
    -- admin identifier to bring the data in
    LEFT JOIN int_prescriptions_shipments ms3
    ON p.admin_prescription_id = ms3.admin_prescription_id
        AND ms3.refill_nbr = 0
    LEFT JOIN prescriptions_shipments_first b ON p.rx_number = b.foreign_rx_number
    LEFT JOIN map_pharmacy_medplum_admin ea
        ON p.patient_id = ea.medplum_patient_id
        AND p.prescription_created_date BETWEEN ea.medplum_valid_from AND ea.medplum_valid_through
    LEFT JOIN deleted_prescriptions d
    ON p.external_prescription_id = d.external_prescription_id
    LEFT JOIN first_medication_dispense md
    ON p.external_prescription_id = md.external_prescription_id
    LEFT JOIN pharmacy_shipment_data bp
    ON p.<pharmacy_censored>_prescription_id = bp.rx_number
        -- Only joins when the legacy <pharmacy_censored> table isn't populated with joining data. Since many
        -- of the fields captured from <pharmacy_censored> tables are COALESCEs, we need to ensure that we
        -- aren't capturing one field from one table and a different field from another.
        AND b.foreign_rx_number IS null
)

, final AS (
    SELECT
        TO_HEX(
            MD5(j.external_prescription_id)
        ) AS uuid
        , j.rx_number
        , j.external_prescription_id
        , j.admin_prescription_id
        , j.dosespot_prescription_id
        , j.<pharmacy_legacy_censored>_prescription_id
        , j.<pharmacy_censored>_prescription_id
        , j.prescription_created_date
        , j.prescription_signed_date
        , j.intake_name
        , CASE WHEN j.intake_name IN ('onboarding', 'follow_up', 'guest_onboarding')
            THEN 'king'
            ELSE REGEXP_EXTRACT(j.intake_name, '^([^_]*)')
            END
        AS protocol
        , CASE
            WHEN j.intake_name LIKE '%onboarding%'
                THEN 'New'
            WHEN REGEXP_CONTAINS(j.intake_name, r'^[^_]+_(up|sustain)_')
                OR REGEXP_CONTAINS(j.intake_name, r'^[^_]+_[^_]+_sustain')
                THEN 'Refill'
        END AS new_refill
        , j.prescription_quantity
        , j.prescription_quantity_units
        , j.days_supply
        , j.supply_duration
        , j.authorized_refills
        , j.directions
        , j.pharmacy_instructions
        , j.patient_id
        , COALESCE(
            dc.admin_user_id
            , j.admin_user_id
        ) AS admin_user_id
        , j.medplum_practitioner_id
        , j.signing_physician
        , j.physician_name
        , j.medplum_pharmacy_id
        , j.pharmacy_name_raw
        , j.pharmacy_name
        , j.medication_id
        , j.medication_category
        , j.snomed_medication_code
        , j.medication_name
        , j.dosespot_prescription_template_note
        , j.shipment_date
        , j.delivered_at
        , j.tracking_number
        , j.shipment_carrier
        , j.<pharmacy_censored>_shipment_date
        , CASE WHEN j.pharmacy_name = '<censored>'
            THEN j.admin_shipment_date END
        AS family_everwell_shipment_date
        , CASE WHEN j.pharmacy_name = '<censored>'
            THEN j.admin_shipment_date END
        AS tailormade_shipment_date
        , CASE WHEN j.pharmacy_name = '<censored>'
            THEN j.admin_shipment_date END
        AS precision_shipment_date
        , j.admin_shipment_date
        , j.shpmt_updated
        , j.<pharmacy_censored>_prescription_status
        , j.operator_type
        , CASE WHEN j.shipment_date IS NOT null
            THEN 1 ELSE 0 END
        AS has_shipment_info
        , j.is_atypical_pharmacy
        , j.contains_bb
        , j.is_deleted
        , j.is_cancelled
    FROM joined j
    LEFT JOIN dim_customers dc
    ON j.patient_id = dc.medplum_patient_id
        -- Rare but there are prescriptions where a shipment date is present but a
        -- signed date isn't, in which case we use the shipment date to determine
        -- which admin user id the patient id belongs to.
        AND COALESCE(j.prescription_created_date, j.shipment_date)
        BETWEEN dc.medplum_valid_from AND dc.medplum_valid_through
)

SELECT * FROM final
