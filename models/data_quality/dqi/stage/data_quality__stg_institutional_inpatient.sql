

    select *
    from {{ ref('input_layer__medical_claim') }}
    where claim_type = 'institutional'
    and {{ substring('bill_type_code', 1, 2) }} = '11'
