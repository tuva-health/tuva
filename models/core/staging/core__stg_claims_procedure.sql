-- depends_on: {{ ref('data_quality__claims_preprocessing_summary') }}

{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
   )
}}

-- *************************************************
-- This dbt model creates the procedure table in core.
-- *************************************************




with unpivot_cte as (

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_1 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_1 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_1 is not null
    
union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_2 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_2 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_2 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_3 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_3 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_3 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_4 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_4 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_4 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_5 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_5 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_5 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_6 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_6 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_6 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_7 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_7 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_7 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_8 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_8 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_8 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_9 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_9 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_9 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_10 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_10 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_10 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_11 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_11 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_11 is not null
union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_12 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_12 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_12 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_13 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_13 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_13 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_14 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_14 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_14 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_15 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_15 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_15 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_16 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_16 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_16 is not null
    
union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_17 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_17 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_17 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_18 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_18 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_18 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_19 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_19 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_19 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_20 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_20 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_20 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_21 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_21 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_21 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_22 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_22 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_22 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_23 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_23 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_23 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_24 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_24 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_24 is not null

union distinct

select
  claim_id as claim_id
  , patient_id as patient_id
  , procedure_date_25 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_25 as source_code
  , rendering_npi as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_25 is not null

)


select distinct
    cast(null as {{ dbt.type_string() }} ) as procedure_id
    , cast(unpivot_cte.patient_id as {{ dbt.type_string() }} ) as patient_id
    , cast(coalesce(ap.encounter_id, ed.encounter_id) as {{ dbt.type_string() }} ) as encounter_id
    , cast(unpivot_cte.claim_id as {{ dbt.type_string() }} ) as claim_id
    , {{ try_to_cast_date('unpivot_cte.procedure_date', 'YYYY-MM-DD') }} as procedure_date
    , cast(unpivot_cte.source_code_type as {{ dbt.type_string() }} ) as source_code_type
    , cast(unpivot_cte.source_code as {{ dbt.type_string() }} ) as source_code
    , cast(null as {{ dbt.type_string() }} ) as source_description
    , cast(case
        when icd.icd_10_pcs is not null then 'icd-10-pcs'
    end as {{ dbt.type_string() }} ) as normalized_code_type
    , cast(icd.icd_10_pcs as {{ dbt.type_string() }} ) as normalized_code
    , cast(icd.description as {{ dbt.type_string() }} ) as normalized_description
    , cast(unpivot_cte.modifier_1 as {{ dbt.type_string() }} ) as modifier_1
    , cast(unpivot_cte.modifier_2 as {{ dbt.type_string() }} ) as modifier_2
    , cast(unpivot_cte.modifier_3 as {{ dbt.type_string() }} ) as modifier_3
    , cast(unpivot_cte.modifier_4 as {{ dbt.type_string() }} ) as modifier_4
    , cast(null as {{ dbt.type_string() }} ) as modifier_5
    , cast(unpivot_cte.practitioner_npi as {{ dbt.type_string() }} ) as practitioner_id
    , cast(unpivot_cte.data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run
from unpivot_cte
  left join {{ ref('terminology__icd_10_pcs') }} as icd
    on unpivot_cte.source_code = icd.icd_10_pcs
left join {{ ref('acute_inpatient__encounter_id')}} as ap
    on  unpivot_cte.claim_id = ap.claim_id
left join {{ ref('emergency_department__int_encounter_id')}} as ed
    on  unpivot_cte.claim_id = ed.claim_id
