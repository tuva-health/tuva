SELECT 
    *
    , {{ dbt.concat(["person_id", "'|'", "data_source"]) }} as patient_source_key
FROM {{ ref('core__patient')}}