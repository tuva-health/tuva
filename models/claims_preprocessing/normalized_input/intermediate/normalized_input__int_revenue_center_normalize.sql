{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


select
    claim_id
    , claim_line_number
    , data_source
    , rev.revenue_center_code as normalized_code
    , rev.revenue_center_description as normalized_description
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('normalized_input__stg_medical_claim') }} as med
left outer join {{ ref('terminology__revenue_center') }} as rev
    {% if target.type == 'fabric' %}
        on RIGHT(REPLICATE('0', 4) + med.revenue_center_code, 4) = rev.revenue_center_code
    {% else %}
        on lpad(med.revenue_center_code, 4, '0') = rev.revenue_center_code
    {% endif %}
where claim_type = 'institutional'
