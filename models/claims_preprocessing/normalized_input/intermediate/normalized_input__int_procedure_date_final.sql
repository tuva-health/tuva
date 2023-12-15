{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
    claim_id
    , data_source
    , max(case when column_name = 'PROCEDURE_DATE_1' then normalized_code else null end) as procedure_date_1
    , max(case when column_name = 'PROCEDURE_DATE_2' then normalized_code else null end) as procedure_date_2
    , max(case when column_name = 'PROCEDURE_DATE_3' then normalized_code else null end) as procedure_date_3
    , max(case when column_name = 'PROCEDURE_DATE_4' then normalized_code else null end) as procedure_date_4
    , max(case when column_name = 'PROCEDURE_DATE_5' then normalized_code else null end) as procedure_date_5
    , max(case when column_name = 'PROCEDURE_DATE_6' then normalized_code else null end) as procedure_date_6
    , max(case when column_name = 'PROCEDURE_DATE_7' then normalized_code else null end) as procedure_date_7
    , max(case when column_name = 'PROCEDURE_DATE_8' then normalized_code else null end) as procedure_date_8
    , max(case when column_name = 'PROCEDURE_DATE_9' then normalized_code else null end) as procedure_date_9
    , max(case when column_name = 'PROCEDURE_DATE_10' then normalized_code else null end) as procedure_date_10
    , max(case when column_name = 'PROCEDURE_DATE_11' then normalized_code else null end) as procedure_date_11
    , max(case when column_name = 'PROCEDURE_DATE_12' then normalized_code else null end) as procedure_date_12
    , max(case when column_name = 'PROCEDURE_DATE_13' then normalized_code else null end) as procedure_date_13
    , max(case when column_name = 'PROCEDURE_DATE_14' then normalized_code else null end) as procedure_date_14
    , max(case when column_name = 'PROCEDURE_DATE_15' then normalized_code else null end) as procedure_date_15
    , max(case when column_name = 'PROCEDURE_DATE_16' then normalized_code else null end) as procedure_date_16
    , max(case when column_name = 'PROCEDURE_DATE_17' then normalized_code else null end) as procedure_date_17
    , max(case when column_name = 'PROCEDURE_DATE_18' then normalized_code else null end) as procedure_date_18
    , max(case when column_name = 'PROCEDURE_DATE_19' then normalized_code else null end) as procedure_date_19
    , max(case when column_name = 'PROCEDURE_DATE_20' then normalized_code else null end) as procedure_date_20
    , max(case when column_name = 'PROCEDURE_DATE_21' then normalized_code else null end) as procedure_date_21
    , max(case when column_name = 'PROCEDURE_DATE_22' then normalized_code else null end) as procedure_date_22
    , max(case when column_name = 'PROCEDURE_DATE_23' then normalized_code else null end) as procedure_date_23
    , max(case when column_name = 'PROCEDURE_DATE_24' then normalized_code else null end) as procedure_date_24
    , max(case when column_name = 'PROCEDURE_DATE_14' then normalized_code else null end) as procedure_date_25
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__int_procedure_date_voting') }}
where (occurrence_row_count = 1
        and occurrence_count > next_occurrence_count)
group by
    claim_id
    , data_source
