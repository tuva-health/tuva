{{ config(
     tags = ['data_assets', 'package_invariant', 'terminology'],
     severity = 'error'
   )
}}

select
    fipscounty
  , ssa_code
  , count(*) as row_count
from {{ ref('terminology__ssa_fips_state_county_crosswalk') }}
group by
    fipscounty
  , ssa_code
having
    (fipscounty is null and ssa_code is null)
    or count(*) > 1
