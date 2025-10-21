select 
      RIGHT('00' + prov.specialty_code, 2) as specialty_code
    , prov.specialty_description
    , prov.pecos_specialty_description
    , asgn.primary_care_physician_step1
    , asgn.specialist_physician_step_2
    , coalesce(asgn.physician,1) as physician
    , case when asgn.specialty_code is not null then 1 else 0 end as specialty_used_in_assignment
from {{ref('terminology__cms_acceptable_provider_specialty_codes')}} prov
left join {{ref('cms_provider_attribution__stg_provider_specialty_assignment_codes')}} asgn
    on RIGHT('00' + prov.specialty_code, 2) = asgn.specialty_code