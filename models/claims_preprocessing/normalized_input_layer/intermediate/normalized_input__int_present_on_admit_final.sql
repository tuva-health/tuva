{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
    claim_id
    , data_source
    , max(case when column_name = 'DIAGNOSIS_POA_1' then normalized_code else null end) as diagnosis_poa_1
    , max(case when column_name = 'DIAGNOSIS_POA_2' then normalized_code else null end) as diagnosis_poa_2
    , max(case when column_name = 'DIAGNOSIS_POA_3' then normalized_code else null end) as diagnosis_poa_3
    , max(case when column_name = 'DIAGNOSIS_POA_4' then normalized_code else null end) as diagnosis_poa_4
    , max(case when column_name = 'DIAGNOSIS_POA_5' then normalized_code else null end) as diagnosis_poa_5
    , max(case when column_name = 'DIAGNOSIS_POA_6' then normalized_code else null end) as diagnosis_poa_6
    , max(case when column_name = 'DIAGNOSIS_POA_7' then normalized_code else null end) as diagnosis_poa_7
    , max(case when column_name = 'DIAGNOSIS_POA_8' then normalized_code else null end) as diagnosis_poa_8
    , max(case when column_name = 'DIAGNOSIS_POA_9' then normalized_code else null end) as diagnosis_poa_9
    , max(case when column_name = 'DIAGNOSIS_POA_10' then normalized_code else null end) as diagnosis_poa_10
    , max(case when column_name = 'DIAGNOSIS_POA_11' then normalized_code else null end) as diagnosis_poa_11
    , max(case when column_name = 'DIAGNOSIS_POA_12' then normalized_code else null end) as diagnosis_poa_12
    , max(case when column_name = 'DIAGNOSIS_POA_13' then normalized_code else null end) as diagnosis_poa_13
    , max(case when column_name = 'DIAGNOSIS_POA_14' then normalized_code else null end) as diagnosis_poa_14
    , max(case when column_name = 'DIAGNOSIS_POA_15' then normalized_code else null end) as diagnosis_poa_15
    , max(case when column_name = 'DIAGNOSIS_POA_16' then normalized_code else null end) as diagnosis_poa_16
    , max(case when column_name = 'DIAGNOSIS_POA_17' then normalized_code else null end) as diagnosis_poa_17
    , max(case when column_name = 'DIAGNOSIS_POA_18' then normalized_code else null end) as diagnosis_poa_18
    , max(case when column_name = 'DIAGNOSIS_POA_19' then normalized_code else null end) as diagnosis_poa_19
    , max(case when column_name = 'DIAGNOSIS_POA_20' then normalized_code else null end) as diagnosis_poa_20
    , max(case when column_name = 'DIAGNOSIS_POA_21' then normalized_code else null end) as diagnosis_poa_21
    , max(case when column_name = 'DIAGNOSIS_POA_22' then normalized_code else null end) as diagnosis_poa_22
    , max(case when column_name = 'DIAGNOSIS_POA_23' then normalized_code else null end) as diagnosis_poa_23
    , max(case when column_name = 'DIAGNOSIS_POA_24' then normalized_code else null end) as diagnosis_poa_24
    , max(case when column_name = 'DIAGNOSIS_POA_25' then normalized_code else null end) as diagnosis_poa_25
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__int_present_on_admit_voting') }}
where (occurrence_row_count = 1
        and occurrence_count > next_occurrence_count)
group by
    claim_id
    , data_source
