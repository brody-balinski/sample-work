WITH medication_requests AS (
    SELECT * FROM {{ source('<censored>', 'medication_requests') }}
)

, stg_<censored>_medications AS (
    SELECT * FROM {{ ref('stg_<censored>_medications') }}
)

, stg_organizations AS (
    SELECT * FROM {{ ref('stg_organizations') }}
)

, semifinal AS (
    SELECT
        mr.id AS external_prescription_id
        , CASE WHEN mr.identifier_system_1 = 'maximus' THEN mr.identifier_value_1
            WHEN mr.identifier_system_2 = 'maximus' THEN mr.identifier_value_2
        END AS admin_prescription_id
        , CASE WHEN mr.identifier_system_1 = 'dosespot' THEN mr.identifier_value_1
            WHEN mr.identifier_system_2 = 'dosespot' THEN mr.identifier_value_2
        END AS dosespot_prescription_id
        , CASE WHEN mr.identifier_system_1 = 'elation' THEN mr.identifier_value_1
            WHEN mr.identifier_system_2 = 'elation' THEN mr.identifier_value_2
        END AS elation_prescription_id
        , CASE WHEN mr.identifier_system_1 = 'belmar' THEN mr.identifier_value_1
            WHEN mr.identifier_system_2 = 'belmar' THEN mr.identifier_value_2
        END AS belmar_prescription_id
        , SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', mr.authoredon) AS prescription_created_date
        -- Change to actual signed field when available
        , SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', mr.authoredon) AS prescription_signed_date
        , mr.extension_id
        , mr.extension_valuestring
        , mr.dispenserequest_quantity_value AS prescription_quantity
        , mr.dispenserequest_quantity_unit AS prescription_quantity_units
        , SAFE_CAST(mr.dispenserequest_expectedsupplyduration_value AS INTEGER) AS days_supply
        , mr.dispenserequest_expectedsupplyduration_unit AS supply_duration
        , CAST(mr.dispenserequest_numberofrepeatsallowed AS INTEGER) AS authorized_refills
        , mr.dosageinstruction_patientinstruction AS directions
        , CASE
            WHEN mr.note_id_1 = 'dosespot-prescription-pharmacy-note'
                THEN mr.note_text_1
            WHEN mr.note_id_2 = 'dosespot-prescription-pharmacy-note'
                THEN mr.note_text_2
        END AS pharmacy_instructions
        , REGEXP_EXTRACT(mr.subject_reference, r'Patient/(.*)') AS patient_id
        , REGEXP_EXTRACT(mr.requester_reference, r'Practitioner/(.*)') AS <censored>_practitioner_id
        , '' AS physician_name
        , REGEXP_EXTRACT(
            mr.dispenserequest_performer_reference, r'Organization/(.*)'
        ) AS <censored>_pharmacy_id
        , '' AS pharmacy_name_raw
        , '' AS pharmacy_name
        , COALESCE(mr.status = 'cancelled', FALSE) AS is_cancelled
        , CASE WHEN mr.medicationcodeableconcept_coding_system = 'dosespot-favorite-id'
            AND mr.medicationcodeableconcept_coding_code != '0'
            THEN CAST(mr.medicationcodeableconcept_coding_code AS STRING)
        END AS medication_id
        , CASE WHEN mr.medicationcodeableconcept_coding_system = 'http://snomed.info/sct'
            THEN mr.medicationcodeableconcept_coding_code
        END AS snomed_medication_code
        , mr.medicationcodeableconcept_coding_display AS medication_name
        , LOWER(
            CASE WHEN note_id_1 = 'dosespot-prescription-template-note' THEN note_text_1
            WHEN note_id_2 = 'dosespot-prescription-template-note' THEN note_text_2
            END
        ) AS dosespot_prescription_template_note
        , '' AS signing_physician
        , mr.meta_lastupdated AS api_last_updated
        , mr._sync_timestamp AS extracted_at
    FROM medication_requests mr
)

, final AS (
    SELECT
        sf.dosespot_prescription_id AS rx_number
        , sf.external_prescription_id
        , sf.admin_prescription_id
        , sf.dosespot_prescription_id
        , sf.elation_prescription_id
        , sf.belmar_prescription_id
        , sf.prescription_created_date
        , sf.prescription_signed_date
        , sf.prescription_quantity
        , sf.prescription_quantity_units
        , sf.days_supply
        , sf.supply_duration
        , sf.authorized_refills
        , sf.directions
        , sf.pharmacy_instructions
        , sf.patient_id
        , sf.<censored>_practitioner_id
        , sf.<censored>_pharmacy_id
        , o.pharmacy_name AS pharmacy_name_raw
        -- Pharmacy names are standardized in the mepdlum organizations staging table; both
        -- to match historical reporting and avoid instances where pharmacies share a similar
        -- but different name (e.g. 'Red Rock Springville' and 'Red Rock St. George')
        , COALESCE(
            o.rpt_pharmacy_name
            , 'Other'
        ) AS pharmacy_name
        , sf.medication_id
        , sf.snomed_medication_code
        -- extension_valuestring sometimes contains the medication name, rather than
        -- medicationcodeableconcept_coding_display. Although rare but in some instances,
        -- particularly with Semaglutide / Glycine compounds, medication name from medication
        -- requests begins with a dosage. In such cases, we'll default to considering
        -- Medication Knowledge (the dim table for medications) as the source of truth for
        -- medication name.
        , CASE
            WHEN COALESCE(sf.medication_name, '') != mk.medication_name
                AND mk.medication_name IS NOT null
                THEN mk.medication_name
            WHEN (
                sf.medication_name IN ('None', 'Unknown Medication')
                OR sf.medication_name IS null
            )
                AND sf.extension_id = 'elation-medication-name'
                AND sf.extension_valuestring IS NOT null
                THEN sf.extension_valuestring
            ELSE sf.medication_name
            END
        AS medication_name
        , IFNULL(
            REGEXP_EXTRACT(
                sf.dosespot_prescription_template_note
                , r'^(.*) \('
            )
            , sf.dosespot_prescription_template_note
        ) AS dosespot_prescription_template_note
        , sf.<censored>_practitioner_id AS signing_physician
        , sf.is_cancelled
        , sf.api_last_updated
        , sf.extracted_at
    FROM semifinal sf
    LEFT JOIN stg_<censored>_medications mk
    ON sf.medication_id = mk.dosespot_medication_id
    LEFT JOIN stg_organizations o
    ON sf.<censored>_pharmacy_id = o.<censored>_pharmacy_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sf.external_prescription_id
        ORDER BY sf.extracted_at DESC
    ) = 1
)

SELECT * FROM final
