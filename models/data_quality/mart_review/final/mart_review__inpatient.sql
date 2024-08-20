{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

WITH cte AS (
    SELECT DISTINCT location_id, npi, name
    FROM {{ ref('core__location')}}
)

SELECT e.*,
    {{ dbt.concat([
        'e.patient_id',
        "'|'",
        'e.data_source'
    ]) }} as patient_source_key,
    {{ dbt.concat([
        'e.encounter_id',
        "'|'",
        'e.data_source'
    ]) }} as encounter_source_key,
    {{ dbt.concat([
        'e.ms_drg_code',
        "' | '",
        'e.ms_drg_description'
    ]) }} as drgwithdescription,
    {{ dbt.concat([
        'e.primary_diagnosis_code',
        "' | '",
        'e.primary_diagnosis_description'
    ]) }} as primary_diagnosis_and_description,
    {{ dbt.concat([
        'e.admit_source_code',
        "' | '",
        'e.admit_source_description'
    ]) }} as admit_source_code_and_description,
    {{ dbt.concat([
        'e.admit_type_code',
        "' | '",
        'e.admit_type_description'
    ]) }} as admit_type_code_and_description,
    {{ dbt.concat([
        'e.discharge_disposition_code',
        "' | '",
        'e.discharge_disposition_description'
    ]) }} as discharge_code_and_description,
    p.ccsr_parent_category,
    p.ccsr_category,
    p.ccsr_category_description,
    {{ dbt.concat([
        'p.ccsr_category',
        "' | '",
        'p.ccsr_category_description'
    ]) }} as ccsr_category_and_description,
    b.body_system
from {{ ref('core__encounter')}} e
left join cte l on e.facility_id = l.location_id
left join {{ ref('ccsr__dx_vertical_pivot') }} p on e.primary_diagnosis_code = p.code and p.ccsr_category_rank = 1
left join {{ ref('ccsr__dxccsr_v2023_1_body_systems') }} b on p.ccsr_parent_category = b.ccsr_parent_category
where e.encounter_type = 'acute inpatient'
