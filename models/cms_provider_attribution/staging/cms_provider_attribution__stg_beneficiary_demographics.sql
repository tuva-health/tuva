/* 
Determine all eligible beneficiaries for claims-based assignment.

The AALR provides fields neeeded to determine eligibility for prospective assignment, but not retrospective assignment.
Therefore, this file does not use AALR, but medical claims to determine eligibility.

Instructions can be found in the CMS SHARED SAVINGS AND LOSSES, ASSIGNMENT AND QUALITY PERFORMANCE STANDARD METHODOLOGY (SLAAM) for a given year.
All of the page references for the SLAAM below are referring to the 2025 SLAAM here: https://www.cms.gov/files/document/medicare-shared-savings-program-shared-savings-and-losses-and-assignment-methodology-specifications.pdf-4
This code should be reviewed yearly to ensure it stays up to date with the yearly SLAAM releases.

The assignable beneficiaries models are based on pages 12-13 of the SLAAM.

Can also be found at the following eCFR link: https://www.ecfr.gov/current/title-42/chapter-IV/subchapter-B/part-425/subpart-E/section-425.401
*/

with extract_fields as (
select 
    elig.*
  , SUBSTRING(file_name, 
      CHARINDEX('.D', file_name) - 3, 3
    ) as performance_year_base
  , case 
      when CHARINDEX('P.A', file_name) > 0 
        then CONCAT('A',
    SUBSTRING(file_name, CHARINDEX('P.A', file_name) + 3, 4
              ))
      else '{{var("aco_id")}}'
    end as aco_id   
from {{ref('input_layer__eligibility')}} elig
)

, add_fields as (
select
    extr.*  
  , 2000 + substring(performance_year_base,2,2) as performance_year 
  , case when upper(performance_year_base) like 'R%' then 1 else 0 end as runout_file
from extract_fields extr
)

select 
      aco_id
    , coalesce(performance_year, reference_year) as performance_year
    , person_id
    , medicare_entitlement_buyin_indicator
    , state
    , runout_file
    , enrollment_start_date as coverage_month
    , death_date
    , file_name
from add_fields
where 1=1
  and performance_year = {{ var('performance_year') }}
  and aco_id = '{{ var("aco_id") }}'
