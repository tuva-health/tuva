{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',False))
   )
}}

{% if var('clinical_enabled', False) == true and var('claims_enabled', False) == true -%}
SELECT
    DATA_SOURCE,
	SOURCE_DATE,
	TABLE_NAME,
	DRILL_DOWN_KEY,
	DRILL_DOWN_VALUE,
	CLAIM_TYPE,
	FIELD_NAME,
	BUCKET_NAME,
	INVALID_REASON,
	FIELD_VALUE,
	SUMMARY_SK
FROM {{ ref('data_quality__data_quality_claims_detail') }}

UNION

SELECT
    DATA_SOURCE,
	SOURCE_DATE,
	TABLE_NAME,
	DRILL_DOWN_KEY,
	DRILL_DOWN_VALUE,
	'CLINICAL' AS CLAIM_TYPE,
	FIELD_NAME,
	BUCKET_NAME,
	INVALID_REASON,
	FIELD_VALUE,
	SUMMARY_SK
FROM {{ ref('data_quality__data_quality_clinical_detail') }}

{% elif var('claims_enabled', False) == true -%}

SELECT
    DATA_SOURCE,
	SOURCE_DATE,
	TABLE_NAME,
	DRILL_DOWN_KEY,
	DRILL_DOWN_VALUE,
	CLAIM_TYPE,
	FIELD_NAME,
	BUCKET_NAME,
	INVALID_REASON,
	FIELD_VALUE,
	SUMMARY_SK
FROM {{ ref('data_quality__data_quality_claims_detail') }}

{% elif var('clinical_enabled', False) == true -%}

SELECT
    DATA_SOURCE,
	SOURCE_DATE,
	TABLE_NAME,
	DRILL_DOWN_KEY,
	DRILL_DOWN_VALUE,
	'CLINICAL' AS CLAIM_TYPE,
	FIELD_NAME,
	BUCKET_NAME,
	INVALID_REASON,
	FIELD_VALUE,
	SUMMARY_SK
FROM {{ ref('data_quality__data_quality_clinical_detail') }}

{%- endif %}