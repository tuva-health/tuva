select * from {{ ref('header_validation__int_admit_source_voting') }}
where (occurrence_row_count = 1
        and admit_source_occurrence_count > next_occurrence_count)