select distinct
    substring(cast(fipscounty as varchar),1,2) as fips_state
  , substring(cast(ssa_code as varchar),1,2) as ssa_state
from {{ref('terminology__ssa_fips_state_county_crosswalk')}}