-- Code to find duplicate encounters
-- Tests 3/4 conditions in the acute_inpatient__generate_encounter_id model (which is duplicated across other encounter models)
with duplicate_start_date as (
select 
      person_id
    , encounter_type
    , encounter_start_date
    , count(*)
from {{ ref('core__encounter') }}
group by 
      person_id
    , encounter_type
    , encounter_start_date
having
    count(*) > 1
)

, duplicate_end_date as (
select 
      person_id
    , encounter_type
    , encounter_end_date
    , count(*)
from {{ ref('core__encounter') }}
group by 
      person_id
    , encounter_type
    , encounter_end_date
having
    count(*) > 1
)

, overlapping_encounters as (
select 
      e1.encounter_id as encounter_id_1
    , e1.person_id
    , e1.encounter_start_date as start_date_1
    , e1.encounter_type
    , e1.encounter_end_date as end_date_1
    , e2.encounter_id as encounter_id_2
    , e2.encounter_start_date as start_date_2
    , e2.encounter_end_date as end_date_2
from {{ ref('core__encounter') }} e1
inner join {{ ref('core__encounter') }} e2
    on e1.person_id = e2.person_id
    and e1.encounter_id != e2.encounter_id  -- avoid duplicate pairs
-- overlapping condition: e2 starts during e1's stay
where e2.encounter_start_date between e1.encounter_start_date and e1.encounter_end_date
    and e2.encounter_start_date != e1.encounter_start_date  -- exclude same-day starts (handled in duplicate_start_date)
)

select person_id from duplicate_start_date
union
select person_id from duplicate_end_date
union
select person_id from overlapping_encounters
;
