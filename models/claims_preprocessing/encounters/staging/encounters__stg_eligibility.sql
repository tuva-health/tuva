with normalized_input__eligibility as (
    select *
    from {{ ref('the_tuva_project', 'normalized_input__eligibility') }}
)
select
    eligibility_sk
    , data_source
    , member_id
    , birth_date
    , gender
    , race
    , row_number() over (partition by data_source, member_id order by e.enrollment_start_date desc) as patient_row_num
from normalized_input__eligibility as e
