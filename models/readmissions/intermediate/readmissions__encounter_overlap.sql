{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

-- Here we give a list of all pairs of encounters
-- that have some date overlap.


with encounters_with_row_num as (
select
    encounter_id
    , person_id
    , admit_date
    , discharge_date
    , row_number() over (
        partition by person_id
order by encounter_id
	) as row_num
from {{ ref('readmissions__encounter') }}
)


, cartesian as (
select
    aa.encounter_id as encounter_id_a
    , bb.encounter_id as encounter_id_b
    , aa.person_id
    , aa.admit_date as ai
    , aa.discharge_date as af
    , bb.admit_date as bi
    , bb.discharge_date as bf
    , case
        when (aa.admit_date between bb.admit_date and bb.discharge_date) or (aa.discharge_date between bb.admit_date and bb.discharge_date) or
             (bb.admit_date between aa.admit_date and aa.discharge_date) or (bb.discharge_date between aa.admit_date and aa.discharge_date)
        then 1
        else 0
    end as overlap
    from encounters_with_row_num as aa
    left outer join encounters_with_row_num as bb
    on aa.person_id = bb.person_id and aa.row_num < bb.row_num
)


, overlapping_pairs as (
    select
        person_id
        , encounter_id_a
	, encounter_id_b
    from cartesian
    where overlap = 1
)



select *, '{{ var('tuva_last_run') }}' as tuva_last_run
from overlapping_pairs
