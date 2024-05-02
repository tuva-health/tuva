{{ config(
     enabled = var('pqi_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

SELECT e.data_source
    ,e.encounter_id
    ,e.exclusion_reason
    ,e.exclusion_number
    ,1 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_01_exclusions') }} e

UNION

SELECT e.data_source
    ,e.encounter_id
    ,e.exclusion_reason
    ,e.exclusion_number
    ,3 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_03_exclusions') }} e

UNION

SELECT e.data_source
    ,e.encounter_id
    ,e.exclusion_reason
    ,e.exclusion_number
    ,5 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_05_exclusions') }} e

UNION

SELECT e.data_source
    ,e.encounter_id
    ,e.exclusion_reason
    ,e.exclusion_number
    ,7 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_07_exclusions') }} e

UNION

SELECT e.data_source
    ,e.encounter_id
    ,e.exclusion_reason
    ,e.exclusion_number
    ,8 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_08_exclusions') }} e

UNION

SELECT e.data_source
    ,e.encounter_id
    ,e.exclusion_reason
    ,e.exclusion_number
    ,11 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_11_exclusions') }} e

UNION

SELECT e.data_source
    ,e.encounter_id
    ,e.exclusion_reason
    ,e.exclusion_number
    ,12 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_12_exclusions') }} e

UNION

SELECT e.data_source
    ,e.encounter_id
    ,e.exclusion_reason
    ,e.exclusion_number
    ,14 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_14_exclusions') }} e

UNION

SELECT e.data_source
    ,e.encounter_id
    ,e.exclusion_reason
    ,e.exclusion_number
    ,15 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_15_exclusions') }} e

UNION

SELECT e.data_source
    ,e.encounter_id
    ,e.exclusion_reason
    ,e.exclusion_number
    ,16 AS pqi_number
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('ahrq_measures__int_pqi_16_exclusions') }} e
