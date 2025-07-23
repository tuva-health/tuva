with enrollment__patient as (
    select *
    from {{ ref('the_tuva_project', 'enrollment__patient') }}
)
select
    patient_sk
    , birth_date
    , gender
    , race
 from enrollment__patient