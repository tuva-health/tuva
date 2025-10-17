/* SLAAM 2.3.3 - Step 0 (aka 'pre-step')

As a “pre-step” in the claims-based assignment process, CMS identifies all beneficiaries who
had at least one primary care service with a physician who is either (1) both an ACO professional
in the ACO and a primary care physician as defined under § 425.20; or (2) who has one of the
primary specialty designation specified in § 425.402(c) (listed in Appendix C, Table 11).42
42 Refer to § 425.402(b)(1). CMS treats a service reported on an FQHC or RHC claim as a primary care service performed by a
primary care physician, according to § 425.404(b). Beginning in PY 2025, beneficiaries who do
not meet the criteria outlined in the “pre-step” may be eligible for assignment under claims-based
assignment Step 3.
*/

select distinct
    aco_id
  , performance_year
  , person_id
  , aco_professional
from {{ref('cms_provider_attribution__int_assigned_beneficiaries__asgn_windows')}} 
where 1=1
    and in_rolling_12_window = 1
    and (provider_type_for_assignment in ('pcp', 'specialist'))