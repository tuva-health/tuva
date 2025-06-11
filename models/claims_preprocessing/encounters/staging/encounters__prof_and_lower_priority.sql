/*This model unions professional with institutional claims that are "lower priority" (dme/lab/ambulance)
and should be part of a higher priority encounter where one exists. We are unioning professional and these institutional claims
here to access downstream from one place */

{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with cte as (
select
  claim_id
, claim_line_number
, data_source
from {{ ref('encounters__stg_professional') }} as a

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
  scg.claim_id
, scg.claim_line_number
, scg.data_source
from {{ ref('service_category__service_category_grouper') }} as scg
where duplicate_row_number = 1
and service_category_2 in ('lab', 'durable medical equipment', 'ambulance')
)

select distinct
  claim_id
, claim_line_number
, data_source
, '{{ var('tuva_last_run') }}' as tuva_last_run
from cte
