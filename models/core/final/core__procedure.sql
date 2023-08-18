{{ config(
     enabled = var('core_enabled',var('tuva_marts_enabled',True))
   )
}}

-- *************************************************
-- This dbt model creates the procedure table in core.
-- *************************************************




with unpivot_cte as (

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_1 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_1 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_1 is not null
    
union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_2 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_2 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_2 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_3 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_3 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_3 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_4 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_4 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_4 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_5 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_5 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_5 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_6 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_6 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_6 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_7 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_7 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_7 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_8 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_8 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_8 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_9 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_9 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_9 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_10 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_10 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_10 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_11 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_11 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_11 is not null
union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_12 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_12 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_12 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_13 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_13 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_13 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_14 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_14 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_14 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_15 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_15 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_15 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_16 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_16 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_16 is not null
    
union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_17 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_17 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_17 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_18 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_18 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_18 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_19 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_19 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_19 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_20 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_20 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_20 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_21 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_21 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_21 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_22 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_22 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_22 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_23 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_23 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_23 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_24 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_24 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_24 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_25 as procedure_date,
  procedure_code_type as source_code_type,
  procedure_code_25 as source_code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('medical_claim') }} 
where procedure_code_25 is not null

)


select distinct
    null as procedure_id,
    unpivot_cte.patient_id as patient_id,
    eg.encounter_id as encounter_id,
    unpivot_cte.claim_id as claim_id,
    unpivot_cte.procedure_date as procedure_date,
    unpivot_cte.source_code_type as source_code_type,
    unpivot_cte.source_code as source_code,
    null as source_description,
    case
        when icd.icd_10_pcs is not null then 'icd-10-pcs'
    end as normalized_code_type,
    icd.icd_10_pcs as normalized_code,
    icd.description as normalized_description,
    null as modifier_1,
    null as modifier_2,
    null as modifier_3,
    null as modifier_4,
    unpivot_cte.practitioner_npi as practitioner_id,
    '{{ var('tuva_last_run')}}' as tuva_last_run
from unpivot_cte
  left join {{ ref('terminology__icd_10_pcs') }} as icd
    on unpivot_cte.source_code = icd.icd_10_pcs
  left join {{ ref('acute_inpatient__encounter_data_for_medical_claims')}}  as eg
    on  unpivot_cte.claim_id = eg.claim_id
    and unpivot_cte.patient_id = eg.patient_id
