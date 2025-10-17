/* Lived in US or US Territories: Section 2.1, Table 1, D

Beneficiary lived in the United States or U.S. territories and
possessions, based on the most recent available data in
beneficiary records regarding the beneficiary’s residence in the
last month of the assignment window.
*/

with benes as (
select
      bene.aco_id
    , bene.performance_year
    , bene.person_id
    , bene.coverage_month
    , bene.bene_fips_state_cd
    
from {{ref('cms_provider_attribution__stg_beneficiary_demographics')}} as bene
inner join {{ref('cms_provider_attribution__stg_assignment_methodology')}} asgn
    on  bene.aco_id = asgn.aco_id
    and bene.performance_year = asgn.performance_year
)

, bene_latest_month as (
select
      aco_id
    , person_id
    , performance_year
    , max(coverage_month) as max_coverage_month
from benes
group by 
      aco_id
    , person_id
    , performance_year
)

select distinct
      benes.performance_year
    , benes.aco_id
    , benes.person_id
from benes as benes
left join {{ref('reference_data__ansi_fips_state')}} as state
    on benes.bene_fips_state_cd = state.ansi_fips_state_code
inner join bene_latest_month ltst
    on  benes.aco_id = ltst.aco_id
    and benes.person_id = ltst.person_id
    and benes.performance_year = ltst.performance_year
    and benes.coverage_month = ltst.max_coverage_month
where 1=1
    -- Not removing individuals who are missing state information
    and state.ansi_fips_state_code is not null 
        or benes.bene_fips_state_cd is null 
        or benes.bene_fips_state_cd = '00' -- 00 is a dummy filler value
