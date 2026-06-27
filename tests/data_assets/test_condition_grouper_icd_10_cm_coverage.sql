{{ config(
     tags = ['data_assets', 'package_invariant', 'condition_grouper'],
     severity = 'error'
   )
}}

with icd_10_cm_codes as (

    select
        terminology__icd_10_cm.icd_10_cm as code
    from {{ ref('terminology__icd_10_cm') }} as terminology__icd_10_cm

),

condition_grouper_icd_10_cm_codes as (

    select
        code
      , count(*) as mapping_count
    from {{ ref('tuva_condition_grouper_code_map') }}
    where code_system = 'icd-10-cm'
    group by
        code

)

select
    icd_10_cm_codes.code
from icd_10_cm_codes
left join condition_grouper_icd_10_cm_codes
    on icd_10_cm_codes.code = condition_grouper_icd_10_cm_codes.code
where coalesce(condition_grouper_icd_10_cm_codes.mapping_count, 0) != 1
