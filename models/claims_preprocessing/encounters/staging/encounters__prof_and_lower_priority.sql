/*This model unions professional with institutional claims that are "lower priority" (dme/lab/ambulance)
and should be part of a higher priority encounter where one exists. We are unioning professional and these institutional claims
here to access downstream from one place */

{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

with prof_and_low_priority_inst_claims as (
select
  claim_id
, claim_line_number
, data_source
from {{ ref('encounters__stg_professional') }} as a

union

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
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from prof_and_low_priority_inst_claims
