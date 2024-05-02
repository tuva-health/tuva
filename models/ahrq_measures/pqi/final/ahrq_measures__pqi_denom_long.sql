{{ config(
     enabled = var('pqi_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

SELECT d.year_number
    ,d.patient_id
    ,d.data_source
    ,1 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_01_denom') }} d

UNION

SELECT d.year_number
    ,d.patient_id
    ,d.data_source
    ,3 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_03_denom') }} d

UNION

SELECT d.year_number
    ,d.patient_id
    ,d.data_source
    ,5 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_05_denom') }} d

UNION

SELECT d.year_number
    ,d.patient_id
    ,d.data_source
    ,7 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_07_denom') }} d

UNION

SELECT d.year_number
    ,d.patient_id
    ,d.data_source
    ,8 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_08_denom') }} d

UNION

SELECT d.year_number
    ,d.patient_id
    ,d.data_source
    ,11 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_11_denom') }} d

UNION

SELECT d.year_number
    ,d.patient_id
    ,d.data_source
    ,12 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_12_denom') }} d

UNION

SELECT d.year_number
    ,d.patient_id
    ,d.data_source
    ,14 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_14_denom') }} d

UNION

SELECT d.year_number
    ,d.patient_id
    ,d.data_source
    ,15 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_15_denom') }} d

UNION

SELECT d.year_number
    ,d.patient_id
    ,d.data_source
    ,16 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_16_denom') }} d
