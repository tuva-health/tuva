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


{% if var('attribution_claims_source') == "cclf" %}

  select *
  from {{ref('cms_provider_attribution__stg_cclf8')}}

{% elif var('attribution_claims_source') == "bcda" %}

  select *
  from {{ref('cms_provider_attribution__stg_cclf8_bcda')}}


{% endif %}

-- Additions/exclusions for retrospective are based on the same year as the performance year
-- Exclusions for retrospective are also based on the same year as the performance year
where 1=1
  and performance_year = {{ var('performance_year') }}
  and aco_id = {{ var('aco_id') }}