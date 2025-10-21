-- NOTE: This is temporary until this can be integrated into the Tuva project
select
      RIGHT('00' + medicare_specialty_code, 2) as medicare_specialty_code
    , medicare_provider_supplier_type_description
    , provider_taxonomy_code
    , provider_taxonomy_description
from {{ref('terminology__medicare_provider_and_supplier_taxonomy_crosswalk')}}