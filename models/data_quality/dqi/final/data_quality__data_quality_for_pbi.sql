{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',False))
   )
}}

{% if var('clinical_enabled', False) == true and var('claims_enabled', False) == true -%}

select
    data_source
    , field_name
    , table_name
    , claim_type
    , bucket_name
    , field_value
    , drill_down_key
    , drill_down_value
    , invalid_reason
    , summary_sk
    , frequency
	, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('data_quality__data_quality_claims_for_pbi') }}

union all

select
    data_source
    , field_name
    , table_name
    , 'CLINICAL' as claim_type
    , bucket_name
    , field_value
    , drill_down_key
    , drill_down_value
    , invalid_reason
    , summary_sk
    , frequency
	, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('data_quality__data_quality_clinical_for_pbi') }}

{% elif var('claims_enabled', False) == true -%}

SELECT
    data_source,
    field_name,
    table_name,
    claim_type,
    bucket_name,
    field_value,
    drill_down_key,
    drill_down_value,
    invalid_reason,
    summary_sk,
    frequency,
	'{{ var('tuva_last_run') }}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_claims_for_pbi') }}

{% elif var('clinical_enabled', False) == true -%}

SELECT
    data_source,
    field_name,
    table_name,
    'CLINICAL' AS claim_type,
    bucket_name,
    field_value,
    drill_down_key,
    drill_down_value,
    invalid_reason,
    summary_sk,
    frequency,
	'{{ var('tuva_last_run') }}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_clinical_for_pbi') }}

{%- endif %}
