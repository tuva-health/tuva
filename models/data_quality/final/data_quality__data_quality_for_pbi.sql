{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',False))
   )
}}

{% if var('clinical_enabled', False) == true and var('claims_enabled', False) == true -%}

SELECT
    DATA_SOURCE,
    FIELD_NAME,
    TABLE_NAME,
    CLAIM_TYPE,
    BUCKET_NAME,
    FIELD_VALUE,
    DRILL_DOWN_KEY,
    DRILL_DOWN_VALUE,
    INVALID_REASON,
    SUMMARY_SK,
    FREQUENCY,
	'{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_claims_for_pbi') }}

UNION

SELECT
    DATA_SOURCE,
    FIELD_NAME,
    TABLE_NAME,
    'CLINICAL' AS CLAIM_TYPE,
    BUCKET_NAME,
    FIELD_VALUE,
    DRILL_DOWN_KEY,
    DRILL_DOWN_VALUE,
    INVALID_REASON,
    SUMMARY_SK,
    FREQUENCY,
	'{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_clinical_for_pbi') }}

{% elif var('claims_enabled', False) == true -%}

SELECT
    DATA_SOURCE,
    FIELD_NAME,
    TABLE_NAME,
    CLAIM_TYPE,
    BUCKET_NAME,
    FIELD_VALUE,
    DRILL_DOWN_KEY,
    DRILL_DOWN_VALUE,
    INVALID_REASON,
    SUMMARY_SK,
    FREQUENCY,
	'{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_claims_for_pbi') }}

{% elif var('clinical_enabled', False) == true -%}

SELECT
    DATA_SOURCE,
    FIELD_NAME,
    TABLE_NAME,
    'CLINICAL' AS CLAIM_TYPE,
    BUCKET_NAME,
    FIELD_VALUE,
    DRILL_DOWN_KEY,
    DRILL_DOWN_VALUE,
    INVALID_REASON,
    SUMMARY_SK,
    FREQUENCY,
	'{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_clinical_for_pbi') }}

{%- endif %}