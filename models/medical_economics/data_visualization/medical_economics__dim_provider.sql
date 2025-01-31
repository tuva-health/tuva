with provider as (

    select 
          npi
        , entity_type_description
        , primary_specialty_description
        , provider_first_name
        , provider_last_name
        , provider_organization_name
        , parent_organization_name
        , practice_address_line_1
        , practice_address_line_2
        , practice_city
        , practice_zip_code
        , mailing_telephone_number
        , location_telephone_number
        , official_telephone_number
        , last_updated
    from {{ ref('terminology__provider') }}

)

select * 
from provider 