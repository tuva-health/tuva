-- Clinical extension columns must remain null on claims-derived rows in the
-- seven hybrid Core tables.

{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('claims_enabled', false))
            and (the_tuva_project.tuva_boolean_var('clinical_enabled', false)),
     tags = ['extension_columns'],
     severity = 'error'
   )
}}

{%- set passthrough_config = var('passthrough', {}) -%}
{%- set extension_prefix = passthrough_config.get('prefix', 'x_') -%}
{%- set strip_prefix = passthrough_config.get('strip', false) -%}
{%- set extension_column = 'tuva_test_extension' if strip_prefix else extension_prefix ~ 'tuva_test_extension' -%}

select 'encounter' as core_table, 'claims-derived row has a clinical extension value' as failure_reason
from {{ ref('core__encounter') }} as core_row
where lower(core_row.encounter_source_type) = 'claim'
  and core_row.{{ extension_column }} is not null

union all

select 'medication' as core_table, 'claims-derived row has a clinical extension value' as failure_reason
from {{ ref('core__medication') }} as core_row
where lower(core_row.source_type) = 'claims'
  and core_row.{{ extension_column }} is not null

union all

select 'patient' as core_table, 'claims-derived row has a clinical extension value' as failure_reason
from {{ ref('core__patient') }} as core_row
inner join {{ ref('normalized__eligibility_remove_duplicates') }} as claims_row
    on core_row.person_id = claims_row.person_id
   and core_row.data_source = claims_row.data_source
where core_row.{{ extension_column }} is not null

union all

select 'condition' as core_table, 'claims-derived row has a clinical extension value' as failure_reason
from {{ ref('core__condition') }} as core_row
where core_row.{{ extension_column }} is not null
  and exists (
      select 1
      from {{ ref('int_condition_from_claims') }} as claims_row
      where core_row.condition_id = claims_row.condition_id
        and core_row.data_source = claims_row.data_source
  )
  and not exists (
      select 1
      from {{ ref('normalized__condition') }} as clinical_row
      where core_row.condition_id = clinical_row.condition_id
        and core_row.data_source = clinical_row.data_source
  )

union all

select 'procedure' as core_table, 'claims-derived row has a clinical extension value' as failure_reason
from {{ ref('core__procedure') }} as core_row
where core_row.{{ extension_column }} is not null
  and exists (
      select 1
      from {{ ref('int_procedure_from_claims') }} as claims_row
      where core_row.procedure_id = claims_row.procedure_id
        and core_row.data_source = claims_row.data_source
  )
  and not exists (
      select 1
      from {{ ref('normalized__procedure') }} as clinical_row
      where core_row.procedure_id = clinical_row.procedure_id
        and core_row.data_source = clinical_row.data_source
  )

union all

select 'location' as core_table, 'claims-derived row has a clinical extension value' as failure_reason
from {{ ref('core__location') }} as core_row
where core_row.{{ extension_column }} is not null
  and exists (
      select 1
      from {{ ref('core__stg_claims_location') }} as claims_row
      where core_row.location_id = claims_row.location_id
        and (
              core_row.data_source = claims_row.data_source
           or (core_row.data_source is null and claims_row.data_source is null)
        )
  )
  and not exists (
      select 1
      from {{ ref('normalized__location') }} as clinical_row
      where core_row.location_id = clinical_row.location_id
        and (
              core_row.data_source = clinical_row.data_source
           or (core_row.data_source is null and clinical_row.data_source is null)
        )
  )

union all

select 'practitioner' as core_table, 'claims-derived row has a clinical extension value' as failure_reason
from {{ ref('core__practitioner') }} as core_row
where core_row.{{ extension_column }} is not null
  and exists (
      select 1
      from {{ ref('core__stg_claims_practitioner') }} as claims_row
      where core_row.practitioner_id = claims_row.practitioner_id
        and core_row.data_source = claims_row.data_source
  )
  and not exists (
      select 1
      from {{ ref('normalized__practitioner') }} as clinical_row
      where core_row.practitioner_id = clinical_row.practitioner_id
        and core_row.data_source = clinical_row.data_source
  )
