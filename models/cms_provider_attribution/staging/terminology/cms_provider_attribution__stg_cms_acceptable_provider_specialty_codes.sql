-- NOTE: This is temporary until this can be integrated into the Tuva project
select
      RIGHT('00' + specialty_code, 2) as specialty_code
    , specialty_description
    , pecos_specialty_description
from {{ref('terminology__cms_acceptable_provider_specialty_codes')}}