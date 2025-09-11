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
, icd_10_pcs_to_ccsr as (
    select *
    from {{ ref('tuva_data_assets', 'icd_10_pcs_ccsr') }}
)
select
    aa.procedure_sk
    , aa.normalized_code as procedure_code
    , 1 as valid_icd_10_pcs_flag
    , ccsr.ccsr_description
from core__procedure as aa
    left outer join icd_10_pcs_to_ccsr as ccsr
    on aa.normalized_code = ccsr.icd_10_pcs
