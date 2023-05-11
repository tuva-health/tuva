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
  procedure_code_type as code_type,
  procedure_code_1 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_1 is not null
    
union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_2 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_2 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_2 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_3 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_3 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_3 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_4 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_4 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_4 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_5 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_5 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_5 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_6 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_6 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_6 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_7 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_7 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_7 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_8 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_8 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_8 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_9 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_9 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_9 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_10 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_10 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_10 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_11 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_11 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_11 is not null
union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_12 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_12 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_12 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_13 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_13 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_13 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_14 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_14 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_14 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_15 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_15 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_15 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_16 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_16 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_16 is not null
    
union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_17 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_17 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_17 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_18 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_18 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_18 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_19 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_19 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_19 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_20 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_20 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_20 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_21 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_21 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_21 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_22 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_22 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_22 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_23 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_23 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_23 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_24 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_24 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_24 is not null

union distinct

select
  claim_id as claim_id,
  patient_id as patient_id,
  procedure_date_25 as procedure_date,
  procedure_code_type as code_type,
  procedure_code_25 as code,
  rendering_npi as practitioner_npi,
  data_source as data_source
from {{ ref('input_layer__medical_claim') }} 
where procedure_code_25 is not null

)


select distinct
  eg.encounter_id as encounter_id,
  unpivot_cte.claim_id as claim_id,
  unpivot_cte.patient_id as patient_id,
  unpivot_cte.procedure_date as procedure_date,
  unpivot_cte.code_type as code_type,
  unpivot_cte.code as code,
  icd.short_description as description,
  unpivot_cte.practitioner_npi as practitioner_npi,
  unpivot_cte.data_source as data_source
from unpivot_cte
  left join {{ ref('terminology__icd_10_pcs') }} as icd
    on unpivot_cte.code = icd.icd_10_pcs
  left join {{ ref('claims_preprocessing__encounter_data_for_medical_claims')}}  as eg
    on  unpivot_cte.claim_id = eg.claim_id
--     and unpivot_cte.claim_line_number = eg.claim_line_number
    and unpivot_cte.patient_id = eg.patient_id
--     and unpivot_cte.data_source = eg.data_source
