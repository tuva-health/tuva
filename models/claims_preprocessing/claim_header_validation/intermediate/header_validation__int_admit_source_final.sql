select * from {{ ref('header_validation__admit_source_normalize_voting') }}
where (occurrence_row_count = 1
        and admit_source_occurrence_count > next_occurrence_count)