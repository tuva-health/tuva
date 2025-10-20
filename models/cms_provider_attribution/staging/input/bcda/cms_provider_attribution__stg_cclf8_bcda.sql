{{ config(
     enabled = var('claims_preprocessing_enabled', False) and var('attribution_claims_source') == 'bcda'
 | as_bool)
}}

with patient as (
select distinct
    pat_id.value as person_id
  , pat.address_state as ssa_state_cd
  , pat.deceaseddatetime as bene_death_dt
  , cov.id as coverage_id
  , year(pat.file_date) as performance_year
from {{ ref('cms_provider_attribution__stg_patient') }} pat 
left join {{ ref('cms_provider_attribution__stg_patient_identifier') }} pat_id
  on pat.id = pat_id.patient_id
  and pat_id.system = 'http://hl7.org/fhir/sid/us-mbi' 
  and pat_id.type_coding_0_extension_0_valuecoding_code = 'current'
left join {{ ref('cms_provider_attribution__stg_coverage') }} cov
  on concat(pat.resourcetype, '/', pat.id) = cov.beneficiary_reference
)

, extension as (
select distinct
    patient.person_id
  , patient.performance_year
  , patient.ssa_state_cd
  , patient.bene_death_dt
  , replace(buyin.url,'https://bluebutton.cms.gov/resources/variables/buyin','') as buyin_month
  , buyin.valuecoding_code as bene_entlmt_buyin_ind
from patient
inner join {{ ref('cms_provider_attribution__stg_coverage_extension') }} yr
  on patient.coverage_id = yr.coverage_id
  and patient.performance_year = cast(yr.valuedate as int)
  and yr.url = 'https://bluebutton.cms.gov/resources/variables/rfrnc_yr'
inner join {{ ref('cms_provider_attribution__stg_coverage_extension') }} buyin
  on patient.coverage_id = buyin.coverage_id
  and substring(buyin.url,1,len(buyin.url) - 2) = 'https://bluebutton.cms.gov/resources/variables/buyin'
)

select 
    '{{var("aco_id")}}' as aco_id
  , person_id
  , performance_year
  , datefromparts(performance_year, cast(buyin_month as int), 1) as coverage_month
  , bene_entlmt_buyin_ind
  , fips_state as bene_fips_state_cd
  , bene_death_dt
  -- TODO: Find field to map for identifying runout if it exists
  , 0 as runout_file 
from extension ext
inner join {{ref('cms_provider_attribution__stg_fips_ssa_state_map')}} map
  on ext.ssa_state_cd = map.ssa_state
