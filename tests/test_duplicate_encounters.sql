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

-- Limit to only encounters with patient/date/NPI continuity
with encounters_npi_continuity as (
select *
from {{ ref('core__encounter') }}
where encounter_type in ('acute inpatient', 'inpatient hospice', 'inpatient psych', 'inpatient skilled nursing', 'inpatient substance use', 'inpatient rehabilitation')
)

, duplicate_start_date as (
select 
      person_id
    , encounter_type
    , encounter_start_date
    , facility_id
    , count(*)
from encounters_npi_continuity
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
from encounters_npi_continuity
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
from encounters_npi_continuity e1
inner join encounters_npi_continuity e2
    on e1.person_id = e2.person_id
    and e1.encounter_id != e2.encounter_id  -- avoid duplicate pairs
-- overlapping condition: e2 starts during e1's stay
where e2.encounter_start_date between e1.encounter_start_date and e1.encounter_end_date
    and e2.encounter_start_date != e1.encounter_start_date  -- exclude same-day starts (handled in duplicate_start_date)
    and e2.facility_id = e1.facility_id
)

, duplicate_encounters as (
select person_id, encounter_type from duplicate_start_date
union
select person_id, encounter_type from duplicate_end_date
union
select person_id, encounter_type from overlapping_encounters
)

, duplicate_encounter_cts as (
select 
      encounter_type
    , count(distinct person_id) as ct
from duplicate_encounters
group by 
    encounter_type
)

, encounter_cts as (
select 
      encounter_type
    , count(distinct person_id) as total_ct
from encounters_npi_continuity
group by 
      encounter_type    
)

select 
      dup.encounter_type
    , ct
    , total_ct
    , ct/total_ct as ratio
from duplicate_encounter_cts dup
inner join encounter_cts enc
    on dup.encounter_type = enc.encounter_type
-- Encounter types with over 5% of people with duplicate encounters will be flagged. This is likely indicative of an error in mapping to Tuva or an issue in the
-- encounter grouping logic.
where ratio >= .05