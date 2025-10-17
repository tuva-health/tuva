select 
      RIGHT('00' + cast(specialty_code as varchar), 2) as specialty_code
    , description
    , primary_care_physician_step1
    , specialist_physician_step_2
    , physician
from {{ref('cms_provider_attribution__provider_specialty_assignment_codes')}}
