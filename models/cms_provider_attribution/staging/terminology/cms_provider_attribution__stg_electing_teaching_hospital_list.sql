select
      collection_year
    , RIGHT('000000' + ccn, 6) as ccn
    , tin
    , hospital_name
    , pecos_legal_business_name
    , street_address
    , po_box
    , city
    , state
    , zip_code
    , street_address_1
    , street_address_2
    , city_2
    , state_2
    , zip_code_2
from {{source('terminology', 'electing_teaching_hospital_list')}}