select
    claim_id
    , data_source
    , max(case when column_checked = 'diagnosis_code_1_normalized' then diagnosis_normalized else null end) as diagnosis_code_1
    , max(case when column_checked = 'diagnosis_code_2_normalized' then diagnosis_normalized else null end) as diagnosis_code_2
    , max(case when column_checked = 'diagnosis_code_3_normalized' then diagnosis_normalized else null end) as diagnosis_code_3
    , max(case when column_checked = 'diagnosis_code_4_normalized' then diagnosis_normalized else null end) as diagnosis_code_4
    , max(case when column_checked = 'diagnosis_code_5_normalized' then diagnosis_normalized else null end) as diagnosis_code_5
from {{ ref('header_validation__diagnosis_voting') }}
where (occurrence_row_count = 1
and occurrence_count > next_occurrence_count)
group by
    claim_id
    , data_source
