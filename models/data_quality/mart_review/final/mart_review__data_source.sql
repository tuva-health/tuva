SELECT DISTINCT data_source
FROM {{ ref('core__medical_claim')}}
