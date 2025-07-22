-- Here we map every procedure code to its corresponding
-- CCS procedure category.
-- This model may list more than one CCS procedure category
-- per encounter_id because different procedures associated with the
-- encounter (different rows on the stg_procedure model) may have
-- different associated CCS procedure categories.

with core__procedure as (
    select *
    from {{ ref('core__procedure') }}
)
, icd_10_pcs as (
    select *
    from {{ ref('tuva_data_assets', 'icd_10_pcs') }}
)
--, icd_10_pcs_to_ccs as (
--    select *
--    from {{ ref('tuva_data_assets', 'icd_10_pcs') }}
--)

select
    aa.procedure_sk
    , aa.normalized_code as procedure_code
    , case
        when bb.icd_10_pcs is null then 0
        else 1
    end as valid_icd_10_pcs_flag
    --, cc.ccs_procedure_category
from
    core__procedure as aa
    left outer join icd_10_pcs as bb
    on aa.normalized_code = bb.icd_10_pcs
  --  left outer join icd_10_pcs_to_ccs as cc
  --  on aa.normalized_code = cc.icd_10_pcs
where aa.normalized_code_type = 'icd-10-pcs'
