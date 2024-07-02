SELECT DISTINCT data_source
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('core__medical_claim')}}
