SELECT n.data_source
    ,n.patient_id
    ,n.year_number
    ,n.encounter_id
    ,1 AS pqi_number
FROM {{ ref('quality_measures__int_pqi_01_num') }} n

UNION

SELECT n.data_source
    ,n.patient_id
    ,n.year_number
    ,n.encounter_id
    ,3 AS pqi_number
FROM {{ ref('quality_measures__int_pqi_03_num') }} n

UNION

SELECT n.data_source
    ,n.patient_id
    ,n.year_number
    ,n.encounter_id
    ,5 AS pqi_number
FROM {{ ref('quality_measures__int_pqi_05_num') }} n

UNION

SELECT n.data_source
    ,n.patient_id
    ,n.year_number
    ,n.encounter_id
    ,7 AS pqi_number
FROM {{ ref('quality_measures__int_pqi_07_num') }} n

UNION

SELECT n.data_source
    ,n.patient_id
    ,n.year_number
    ,n.encounter_id
    ,8 AS pqi_number
FROM {{ ref('quality_measures__int_pqi_08_num') }} n

UNION

SELECT n.data_source
    ,n.patient_id
    ,n.year_number
    ,n.encounter_id
    ,11 AS pqi_number
FROM {{ ref('quality_measures__int_pqi_11_num') }} n

UNION

SELECT n.data_source
    ,n.patient_id
    ,n.year_number
    ,n.encounter_id
    ,12 AS pqi_number
FROM {{ ref('quality_measures__int_pqi_12_num') }} n

UNION

SELECT n.data_source
    ,n.patient_id
    ,n.year_number
    ,n.encounter_id
    ,14 AS pqi_number
FROM {{ ref('quality_measures__int_pqi_14_num') }} n

UNION

SELECT n.data_source
    ,n.patient_id
    ,n.year_number
    ,n.encounter_id
    ,15 AS pqi_number
FROM {{ ref('quality_measures__int_pqi_15_num') }} n

UNION

SELECT n.data_source
    ,n.patient_id
    ,n.year_number
    ,n.encounter_id
    ,16 AS pqi_number
FROM {{ ref('quality_measures__int_pqi_16_num') }} n

