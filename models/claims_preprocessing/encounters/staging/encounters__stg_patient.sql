with normalized_input__eligibility as (
    select *
    from {{ ref('the_tuva_project', 'normalized_input__eligibility') }}
),
sorted_patient as (
    select
        data_source
        , member_id
        , birth_date
        , gender
        , race
        , row_number() over (partition by data_source, member_id order by e.enrollment_start_date desc) as patient_row_num
    from normalized_input__eligibility as e
)
select
    {{ dbt_utils.generate_surrogate_key(['data_source', 'member_id']) }} as patient_sk
    , birth_date
    , gender
    , race
 from sorted_patient
 where patient_row_num = 1