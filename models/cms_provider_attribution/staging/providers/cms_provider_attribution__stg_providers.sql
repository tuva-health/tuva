/**** Primary Care Physicians: Section 2.1, Table 1, E ****

"Beneficiary had at least one primary care service with a
physician who is an ACO professional in the ACO, and is a
primary care physician (defined in § 425.20) or has a primary
specialty designation specified in § 425.402(c) (listed in
Appendix C, Table 11)."

The SLAAM says that CCNs are to be used from the provider/supplier list and TINs for all else for purposes of 
assignment.
p.17 of the SLAAM, "For ease of reference, in this document we refer to use of the ACO Participant List more
generally for assignment operations, although these references (unless otherwise specified)
refer to use of TINs specified on the certified ACO Participant List, and Method II CAHs, ETA
hospitals, and FQHCs and RHCs identified based on CCNs sourced from PECOS, as specified
on the ACO Provider/Supplier List."
*/

with base as (
select distinct
      cast(prov.npi as varchar) as npi
    , prov_supp.aco_id
    , case when prov_supp.npi is not null then 1 else 0 end as aco_professional
    -- Preference is given to the PECOS list since the terminology__provider source is only the most
    -- recent provider specialty and not the historical specialty like what is preserved in the 
    -- provider/supplier list
    , coalesce(
      case 
        when asgn.specialty_used_in_assignment = 0 then null
        when lower(asgn.primary_care_physician_step1) = 'yes' and asgn.physician = 1 then 'pcp'
        when lower(asgn.specialist_physician_step_2) = 'yes' and asgn.physician = 1 then 'specialist'
        when asgn.physician = 0 then 'npp'
      end
      , case 
          when asgn.specialty_used_in_assignment = 0 then null
          when lower(asgn_all.primary_care_physician_step1) = 'yes' and asgn_all.physician = 1 then 'pcp'
          when lower(asgn_all.specialist_physician_step_2) = 'yes' and asgn_all.physician = 1 then 'specialist'
          when asgn_all.physician = 0 then 'npp'
          -- All those who have a mapping, but aren't specialties used in assignment are 'invalid specialists'
          when asgn_all.specialty_code is null and spec.provider_taxonomy_code is not null then 'invalid_specialist'
      end) as provider_type_for_assignment
from {{ref('cms_provider_attribution__stg_provider_taxonomy_unpivot')}} prov
left join {{ref('cms_provider_attribution__stg_provider_supplier_list')}}  prov_supp
    on  cast(prov.npi as varchar(10)) = prov_supp.npi
    and prov_supp.performance_year = {{ var('performance_year') }}
left join {{ref('cms_provider_attribution__stg_provider_specialty_assignment_list')}} as asgn
    on upper(prov_supp.specialty) = upper(asgn.pecos_specialty_description)
left join {{ref('cms_provider_attribution__stg_medicare_provider_and_supplier_taxonomy_crosswalk')}} as spec
    on prov.taxonomy_code = spec.provider_taxonomy_code
left join {{ref('cms_provider_attribution__stg_provider_specialty_assignment_codes')}} as asgn_all
    on medicare_specialty_code = asgn_all.specialty_code
)

, aco_providers_pivot as (
select
    cast(npi as varchar) as npi
  , cast(aco_id as varchar) as aco_id
  , aco_professional
  , {{ dbt_utils.pivot(
      'provider_type_for_assignment',
      ['pcp', 'specialist', 'npp', 'invalid_specialist']
  ) }}
from base
where provider_type_for_assignment is not null
group by 
      cast(npi as varchar)
    , cast(aco_id as varchar)
    , aco_professional
)

-- Add trumping logic when there is more than 1 provider type
-- When there is a valid specialty that is a pcp specialty code + an invalid specialist code, then exclude that provider
-- E.g. The gastroenterology taxonomy code: 207RG0100X (both 10 and 11)
-- E.g. The anesthesiology taxonomy code: 207L00000X (both 79 and 5)
-- E.g. The orthopedic surgery taxonomy code: 207XX0005X (includes 2,20, and 23)
-- TODO: When there are 2 codes that are valid, include that provider (adding this in reduced overall accuracy)
-- E.g. Interventional cardiology taxonomy code: 207RC0000X (both 6 and 11)
, provider_types as (
select 
      aco_providers_pivot.*
    -- NOTE: This trumping logic is based on my best guess of what CMS does when there is more than 1 specialty for a given taxonomy code
    , case
        when invalid_specialist >= 1 then null
        when pcp >= 1 and specialist >= 1 then 'specialist'
        when pcp >= 1 and npp >= 1 then 'npp'
        when specialist >= 1 and npp >= 1 then 'npp'
        when pcp >= 1 and specialist = 0 and npp = 0 and invalid_specialist = 0 then 'pcp'
        when pcp = 0 and specialist >= 1 and npp = 0 then 'specialist'
        when pcp = 0 and specialist = 0 and npp >= 1 then 'npp'
      end as provider_type_for_assignment 
from aco_providers_pivot
)

select 
      npi
    , aco_id
    , provider_type_for_assignment 
    , aco_professional
from provider_types 
where provider_type_for_assignment is not null
