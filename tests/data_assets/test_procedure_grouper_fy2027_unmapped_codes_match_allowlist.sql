{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

-- Tuva 1.0 intentionally retains an exact, clinically reviewed FY2027 gap
-- while it awaits a compatible grouper source and clinical assignments. This
-- symmetric difference fails for either a newly unmapped current code or an
-- allowlisted code that is mapped, removed, or reclassified as deprecated.

with reviewed_fy2027_gap as (

    select '028F3Z0' as code
    union all select '028F3Z1'
    union all select '028G3Z0'
    union all select '028G3Z1'
    union all select '0RGA0E0'
    union all select '0RGA0EJ'
    union all select '0RGA3E0'
    union all select '0RGA3EJ'
    union all select '0RGA4E0'
    union all select '0RGA4EJ'
    union all select '0SG00E0'
    union all select '0SG00EJ'
    union all select '0SG03E0'
    union all select '0SG03EJ'
    union all select '0SG04E0'
    union all select '0SG04EJ'
    union all select '0SG10E0'
    union all select '0SG10EJ'
    union all select '0SG13E0'
    union all select '0SG13EJ'
    union all select '0SG14E0'
    union all select '0SG14EJ'
    union all select '0SG30E0'
    union all select '0SG30EJ'
    union all select '0SG33E0'
    union all select '0SG33EJ'
    union all select '0SG34E0'
    union all select '0SG34EJ'
    union all select '0W99003'
    union all select '0W99303'
    union all select '0W99403'
    union all select '0W9B003'
    union all select '0W9B303'
    union all select '0W9B403'
    union all select '0W9C003'
    union all select '0W9C303'
    union all select '0W9C403'
    union all select '30233S5'
    union all select '30243S5'
    union all select '3E00XGF'
    union all select '3E01XGF'
    union all select '5A0527D'
    union all select '5A1D701'
    union all select '5A1D801'
    union all select '5A1D901'
    union all select 'B85A2ZZ'
    union all select 'B85B2ZZ'
    union all select 'B85G2ZZ'
    union all select 'B85H2ZZ'
    union all select 'X0HQ05C'
    union all select 'X27P3NC'
    union all select 'X27Q3NC'
    union all select 'X27R3NC'
    union all select 'X27S3NC'
    union all select 'X27T3NC'
    union all select 'X27U3NC'
    union all select 'X28M3DC'
    union all select 'X2H43MC'
    union all select 'X2RF3LC'
    union all select 'X2RG3FC'
    union all select 'X2RH0GC'
    union all select 'X2VJ3HC'
    union all select 'X2VW3JC'
    union all select 'X2VX0KC'
    union all select 'XEZ5XKC'
    union all select 'XEZD3QC'
    union all select 'XEZJ3NC'
    union all select 'XEZU3HC'
    union all select 'XEZZXJC'
    union all select 'XEZZXLC'
    union all select 'XEZZXMC'
    union all select 'XEZZXPC'
    union all select 'XEZZXRC'
    union all select 'XEZZXSC'
    union all select 'XFJB3CC'
    union all select 'XFJB4CC'
    union all select 'XFJB8BC'
    union all select 'XFJD3CC'
    union all select 'XFJD4CC'
    union all select 'XFJD8BC'
    union all select 'XHH80JC'
    union all select 'XRH10NC'
    union all select 'XRH13NC'
    union all select 'XRH14NC'
    union all select 'XRH20NC'
    union all select 'XRH23NC'
    union all select 'XRH24NC'
    union all select 'XRH40NC'
    union all select 'XRH43NC'
    union all select 'XRH44NC'
    union all select 'XRH60NC'
    union all select 'XRH63NC'
    union all select 'XRH64NC'
    union all select 'XW0134C'
    union all select 'XW0336C'
    union all select 'XW0337C'
    union all select 'XW0436C'
    union all select 'XW0Q35C'
    union all select 'XW0U0CC'
    union all select 'XW0V0BC'
    union all select 'XXE2XHC'

)

, current_unmapped_codes as (

    select
        terminology.icd_10_pcs as code
    from {{ ref('terminology__icd_10_pcs') }} as terminology
    left join {{ ref('tuva_procedure_grouper_code_map') }} as code_map
      on terminology.icd_10_pcs = code_map.code
      and code_map.code_system = 'icd-10-pcs'
    where terminology.deprecated = 0
      and code_map.code is null

)

, unexpected_current_gap as (

    select
        'unexpected_current_unmapped_code' as discrepancy
      , current_unmapped_codes.code
    from current_unmapped_codes
    left join reviewed_fy2027_gap
      on current_unmapped_codes.code = reviewed_fy2027_gap.code
    where reviewed_fy2027_gap.code is null

)

, stale_reviewed_gap as (

    select
        'reviewed_gap_code_not_current_and_unmapped' as discrepancy
      , reviewed_fy2027_gap.code
    from reviewed_fy2027_gap
    left join current_unmapped_codes
      on reviewed_fy2027_gap.code = current_unmapped_codes.code
    where current_unmapped_codes.code is null

)

select
    discrepancy
  , code
from unexpected_current_gap

union all

select
    discrepancy
  , code
from stale_reviewed_gap
