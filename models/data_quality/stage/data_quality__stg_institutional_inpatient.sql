    SELECT * 
    FROM {{ ref('medical_claim')}}
    WHERE CLAIM_TYPE = 'institutional'
    AND {{ substring('bill_type_code', 1, 2) }} = '11'