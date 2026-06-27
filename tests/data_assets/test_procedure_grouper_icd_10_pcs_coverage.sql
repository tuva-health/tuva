{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

with icd_10_pcs_codes as (

    select
        terminology__icd_10_pcs.icd_10_pcs as code
    from {{ ref('terminology__icd_10_pcs') }} as terminology__icd_10_pcs
    where coalesce(cast(terminology__icd_10_pcs.deprecated as {{ dbt.type_string() }}), '0') = '0'

),

procedure_grouper_icd_10_pcs_codes as (

    select
        code
      , count(*) as mapping_count
    from {{ ref('tuva_procedure_grouper_code_map') }}
    where code_system = 'icd-10-pcs'
    group by
        code

)

select
    icd_10_pcs_codes.code
from icd_10_pcs_codes
left join procedure_grouper_icd_10_pcs_codes
    on icd_10_pcs_codes.code = procedure_grouper_icd_10_pcs_codes.code
where coalesce(procedure_grouper_icd_10_pcs_codes.mapping_count, 0) != 1
