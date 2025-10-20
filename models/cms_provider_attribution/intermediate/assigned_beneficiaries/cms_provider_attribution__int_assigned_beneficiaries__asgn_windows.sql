/* 
Determine all assignable beneficiaries, this is the pre-step (aka step 0)
Instructions can be found in the CMS SHARED SAVINGS AND LOSSES, ASSIGNMENT AND QUALITY PERFORMANCE STANDARD METHODOLOGY for a given year
All of the page references for the SLAAM below are referring to the 2025 SLAAM here: https://www.cms.gov/files/document/medicare-shared-savings-program-shared-savings-and-losses-and-assignment-methodology-specifications.pdf-4
This code should be reviewed yearly to ensure it stays up to date with the yearly SLAAM releases.
*/

/*
For updates throughout the year, Section 2.3.2.1 Preliminary Prospective Assignment with Retrospective
Reconciliation states:

"Assignment will be updated quarterly based on the most recent 12 or 24 months of data."

This means that this information should be based on a rolling 12 to 24 months.
*/

with asgn_windows as (
select
      asgn.aco_id
    , asgn.performance_year
    , asgn.person_id
    , asgn.assignment_methodology
    -- When date is greater than the start, but less than the end, then rolling 12
    , case 
        -- TEST
        -- when GETDATE() between window.window_start and window.window_end then DATEADD(DAY, 1, DATEADD(YEAR, -1, GETDATE()))
        when GETDATE() between window.window_start and window.window_end then '2024-07-01'
        else window.window_start
      end as rolling_12_window_start
    , case 
        -- TEST
        -- when GETDATE() between window.window_start and window.window_end then GETDATE()
        when GETDATE() between window.window_start and window.window_end then '2025-06-30'
        else window.window_end
      end as rolling_12_window_end
from {{ref('cms_provider_attribution__int_assignable_beneficiaries')}} as asgn
inner join {{ref('cms_provider_attribution__stg_assignment_windows')}} as window
    on  asgn.assignment_methodology = window.assignment_methodology
    and asgn.performance_year = window.performance_year    
    and window.performance_year = window.service_year -- Filter out benchmark years
)

select 
      asgn.aco_id
    , asgn.performance_year
    , asgn.person_id
    , asgn.assignment_methodology
    , asgn.rolling_12_window_start as rolling_12_window_start
    , asgn.rolling_12_window_end as rolling_12_window_end
    , DATEADD(YEAR, -1, asgn.rolling_12_window_start) as rolling_24_window_start
    , asgn.rolling_12_window_end as rolling_24_window_end
    , clms.claim_id
    , clms.claim_line_number
    , clms.claim_start_date
    , clms.file_date
    , clms.hcpcs_code
    , clms.allowed_amount
    , clms.provider_type_for_assignment
    , clms.tin
    , clms.npi
    , clms.ccn
    , clms.aco_professional
    , clms.fqhc_rhc_flag
    , clms.method_ii_cah_flag
    , clms.eta_flag
    , case 
        when clms.claim_start_date between asgn.rolling_12_window_start and asgn.rolling_12_window_end
            then 1 else 0 
      end as in_rolling_12_window
    , case 
        when clms.claim_start_date between DATEADD(YEAR, -1, asgn.rolling_12_window_start) and asgn.rolling_12_window_end
            then 1 else 0 
      end as in_rolling_24_window      
from {{ref('cms_provider_attribution__int_table1_e__primary_care_services_by_valid_providers')}} as clms
inner join asgn_windows as asgn
    on  clms.aco_id = asgn.aco_id
    and  clms.person_id = asgn.person_id
    and clms.performance_year = asgn.performance_year
-- TODO: Potentially filter this down to the past 3 years