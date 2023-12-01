select
    claim_id
    , claim_line_number
    , data_source
    , pos.place_of_service_code as normalized_code
from {{ ref('medical_claim') }} med
left join {{ ref('terminology__place_of_service') }} pos
    on lpad(med.place_of_service_code, 2, '0') = pos.place_of_service_code
where claim_type = 'professional'