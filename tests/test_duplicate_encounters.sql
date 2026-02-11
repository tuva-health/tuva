-- Code to find duplicate encounters
-- Tests 3/4 conditions in the acute_inpatient__generate_encounter_id model (which is duplicated across other encounter models)

{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool,
     tags = ['dqi', 'tuva_dqi_sev_4', 'dqi_service_categories', 'dqi_ccsr', 'dqi_cms_chronic_conditions',
            'dqi_tuva_chronic_conditions', 'dqi_cms_hccs', 'dqi_ed_classification',
            'dqi_financial_pmpm', 'dqi_quality_measures', 'dqi_readmission'],
   )
}}
with duplicate_start_date as (
select 
      person_id
    , encounter_type
    , encounter_start_date
    , facility_id
    , count(*)
from {{ ref('core__encounter') }}
group by 
      person_id
    , encounter_type
    , encounter_start_date
    , facility_id
having
    count(*) > 1
)

, duplicate_end_date as (
select 
      person_id
    , encounter_type
    , encounter_end_date
    , facility_id
    , count(*)
from {{ ref('core__encounter') }}
group by 
      person_id
    , encounter_type
    , encounter_end_date
    , facility_id
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
    and e2.facility_id = e1.facility_id
)

select person_id from duplicate_start_date
union
select person_id from duplicate_end_date
union
select person_id from overlapping_encounters

