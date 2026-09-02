{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

with multiple_sources as (
select distinct person_id
, data_source
from {{ ref('normalized__medical_claim') }}

{{ the_tuva_project.union_distinct() }}

select distinct person_id
, data_source
from {{ ref('normalized__eligibility') }}
)

select
person_id
, data_source
, {{ the_tuva_project.stable_id_hash([
    "'patient data source'",
    'person_id',
    'data_source'
  ]) }} as patient_data_source_id
from multiple_sources
