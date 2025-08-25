{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',False))
   )
}}

{% if var('clinical_enabled', False) == true and var('claims_enabled', False) == true -%}
select
    data_source
	, source_date
	, table_name
	, drill_down_key
	, drill_down_value
	, claim_type
	, field_name
	, bucket_name
	, invalid_reason
	, field_value
	, summary_sk
	, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('data_quality__data_quality_claims_detail') }}

union all

select
    data_source
	, source_date
	, table_name
	, drill_down_key
	, drill_down_value
	, 'CLINICAL' as claim_type
	, field_name
	, bucket_name
	, invalid_reason
	, field_value
	, summary_sk
	, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('data_quality__data_quality_clinical_detail') }}

{% elif var('claims_enabled', False) == true -%}

SELECT
    data_source,
	source_date,
	table_name,
	drill_down_key,
	drill_down_value,
	claim_type,
	field_name,
	bucket_name,
	invalid_reason,
	field_value,
	summary_sk,
	'{{ var('tuva_last_run') }}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_claims_detail') }}

{% elif var('clinical_enabled', False) == true -%}

SELECT
    data_source,
	source_date,
	table_name,
	drill_down_key,
	drill_down_value,
	'clinical' as claim_type,
	field_name,
	bucket_name,
	invalid_reason,
	field_value,
	summary_sk,
	'{{ var('tuva_last_run') }}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_clinical_detail') }}

{%- endif %}
