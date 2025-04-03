{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
) }}

with cte as (
    select distinct
        location_id
      , npi
      , name
    from {{ ref('core__location') }}
)

, final as (
    select
        e.*
      , {{ concat_custom(['e.person_id', "'|'", 'e.data_source']) }} as patient_source_key
      , {{ concat_custom(['e.encounter_id', "'|'", 'e.data_source']) }} as encounter_source_key
      , {{ concat_custom(['e.drg_code', "'|'", 'e.drg_description']) }} as drgwithdescription
      , {{ concat_custom(['e.primary_diagnosis_code', "'|'", 'e.primary_diagnosis_description']) }} as primary_diagnosis_and_description
      , {{ concat_custom(['e.admit_source_code', "'|'", 'e.admit_source_description']) }} as admit_source_code_and_description
      , {{ concat_custom(['e.admit_type_code', "'|'", 'e.admit_type_description']) }} as admit_type_code_and_description
      , {{ concat_custom(['e.discharge_disposition_code', "'|'", 'e.discharge_disposition_description']) }} as discharge_code_and_description
      , p.ccsr_parent_category
      , p.ccsr_category
      , p.ccsr_category_description
      , {{ concat_custom(['p.ccsr_category', "'|'", 'p.ccsr_category_description']) }} as ccsr_category_and_description
      , b.body_system
      , case 
            when e.length_of_stay <= 1 then '1. 0-1 day'
            when e.length_of_stay <= 3 then '2. 2-3 days'
            when e.length_of_stay <= 5 then '3. 4-5 days'
            when e.length_of_stay <= 7 then '4. 6-7 days'
            when e.length_of_stay <= 14 then '5. 8-14 days'
            when e.length_of_stay <= 30 then '6. 15-30 days'
            else '7. 31+ Days'
        end as los_groups
      , weights.drg_weight
    from {{ ref('core__encounter') }} as e
    left join cte as l
      on e.facility_id = l.location_id
    left join {{ ref('ccsr__dx_vertical_pivot') }} as p
      on e.primary_diagnosis_code = p.code
      and p.ccsr_category_rank = 1
    left join {{ ref('ccsr__dxccsr_v2023_1_body_systems') }} as b
      on p.ccsr_parent_category = b.ccsr_parent_category
    left join {{ ref('terminology__ms_drg_weights_los')}} as weights
      on e.drg_code = weights.ms_drg
    where e.encounter_type = 'acute inpatient'
)

select * from final