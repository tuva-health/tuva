{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


select
    claim_id
    , claim_line_number
    , data_source
    , pos.place_of_service_code as normalized_code
    , pos.place_of_service_description as normalized_description
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('normalized_input__stg_medical_claim') }} as med
left outer join {{ ref('terminology__place_of_service') }} as pos
    {% if target.type == 'fabric' %}
        on RIGHT(REPLICATE('0', 2) + med.place_of_service_code, 2) = pos.place_of_service_code
    {% else %}
        on lpad(med.place_of_service_code, 2, '0') = pos.place_of_service_code
    {% endif %}
where claim_type = 'professional'
