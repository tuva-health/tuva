{{ config(
    enabled = var('claims_enabled', False)
) }}



with total_institutional_claims as (
select cast(count(*) as {{ dbt.type_numeric() }}) as totalinst
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
),

total_professional_claims as (
select cast(count(*) as {{ dbt.type_numeric() }}) as totalprof
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'professional'
),

total_claims as (
select cast(count(*) as {{ dbt.type_numeric() }}) as total
from {{ ref('data_quality__header_values') }}
),



-- **************************************************
-- bill_type_code fields:
-- **************************************************

missing_bill_type_code as (
select
  '(inst claims with missing bill type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and missing_bill_type_code = 1
),


populated_bill_type_code as (
select
  '(inst claims with populated bill type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and missing_bill_type_code = 0
),


always_valid_bill_type_code as (
select
  '(inst claims with always valid bill type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and always_valid_bill_type_code = 1
),


valid_and_invalid_bill_type_code as (
select
  '(inst claims with valid and invalid bill type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and valid_and_invalid_bill_type_code = 1
),


always_invalid_bill_type_code as (
select
  '(inst claims with always invalid bill type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and always_invalid_bill_type_code = 1
),


undeterminable_bill_type_code as (
select
  '(inst claims with undeterminable bill type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and undeterminable_bill_type_code = 1
),


determinable_bill_type_code as (
select
  '(inst claims with determinable bill type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and determinable_bill_type_code = 1
),


unique_bill_type_code as (
select
  '(inst claims with unique bill type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and unique_bill_type_code = 1
),


usable_bill_type_code as (
select
  '(inst claims with usable bill type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and usable_bill_type_code = 1
),



-- **************************************************
-- drg_code fields:
-- **************************************************

missing_drg_code as (
select
  '(inst claims with missing drg) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and missing_drg_code = 1
),


populated_drg_code as (
select
  '(inst claims with populated drg) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and missing_drg_code = 0
),


always_valid_drg_code as (
select
  '(inst claims with always valid drg) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and always_valid_drg_code = 1
),


valid_and_invalid_drg_code as (
select
  '(inst claims with valid and invalid drg) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and valid_and_invalid_drg_code = 1
),


always_invalid_drg_code as (
select
  '(inst claims with always invalid drg) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and always_invalid_drg_code = 1
),


undeterminable_drg_code as (
select
  '(inst claims with undeterminable drg) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and undeterminable_drg_code = 1
),


determinable_drg_code as (
select
  '(inst claims with determinable drg) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and determinable_drg_code = 1
),


unique_drg_code as (
select
  '(inst claims with unique drg) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and unique_drg_code = 1
),


usable_drg_code as (
select
  '(inst claims with usable drg) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and usable_drg_code = 1
),



-- **************************************************
-- admit_type_code fields:
-- **************************************************

missing_admit_type_code as (
select
  '(inst claims with missing admit type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and missing_admit_type_code = 1
),


populated_admit_type_code as (
select
  '(inst claims with populated admit type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and missing_admit_type_code = 0
),


always_valid_admit_type_code as (
select
  '(inst claims with always valid admit type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and always_valid_admit_type_code = 1
),


valid_and_invalid_admit_type_code as (
select
  '(inst claims with valid and invalid admit type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and valid_and_invalid_admit_type_code = 1
),


always_invalid_admit_type_code as (
select
  '(inst claims with always invalid admit type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and always_invalid_admit_type_code = 1
),


undeterminable_admit_type_code as (
select
  '(inst claims with undeterminable admit type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and undeterminable_admit_type_code = 1
),


determinable_admit_type_code as (
select
  '(inst claims with determinable admit type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and determinable_admit_type_code = 1
),


unique_admit_type_code as (
select
  '(inst claims with unique admit type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and unique_admit_type_code = 1
),


usable_admit_type_code as (
select
  '(inst claims with usable admit type) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and usable_admit_type_code = 1
),



-- **************************************************
-- admit_source_code fields:
-- **************************************************

missing_admit_source_code as (
select
  '(inst claims with missing admit source) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and missing_admit_source_code = 1
),


populated_admit_source_code as (
select
  '(inst claims with populated admit source) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and missing_admit_source_code = 0
),


always_valid_admit_source_code as (
select
  '(inst claims with always valid admit source) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and always_valid_admit_source_code = 1
),


valid_and_invalid_admit_source_code as (
select
  '(inst claims with valid and invalid admit source) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and valid_and_invalid_admit_source_code = 1
),


always_invalid_admit_source_code as (
select
  '(inst claims with always invalid admit source) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and always_invalid_admit_source_code = 1
),


undeterminable_admit_source_code as (
select
  '(inst claims with undeterminable admit source) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and undeterminable_admit_source_code = 1
),


determinable_admit_source_code as (
select
  '(inst claims with determinable admit source) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and determinable_admit_source_code = 1
),


unique_admit_source_code as (
select
  '(inst claims with unique admit source) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and unique_admit_source_code = 1
),


usable_admit_source_code as (
select
  '(inst claims with usable admit source) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and usable_admit_source_code = 1
),



-- **************************************************
-- discharge_disposition_code fields:
-- **************************************************

missing_discharge_disposition_code as (
select
  '(inst claims with missing discharge disp) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and missing_discharge_disposition_code = 1
),


populated_discharge_disposition_code as (
select
  '(inst claims with populated discharge disp) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and missing_discharge_disposition_code = 0
),


always_valid_discharge_disposition_code as (
select
  '(inst claims with always valid discharge disp) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and always_valid_discharge_disposition_code = 1
),


valid_and_invalid_discharge_disposition_code as (
select
  '(inst claims with valid and invalid discharge disp) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and valid_and_invalid_discharge_disposition_code = 1
),


always_invalid_discharge_disposition_code as (
select
  '(inst claims with always invalid discharge disp) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and always_invalid_discharge_disposition_code = 1
),


undeterminable_discharge_disposition_code as (
select
  '(inst claims with undeterminable discharge disp) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and undeterminable_discharge_disposition_code = 1
),


determinable_discharge_disposition_code as (
select
  '(inst claims with determinable discharge disp) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and determinable_discharge_disposition_code = 1
),


unique_discharge_disposition_code as (
select
  '(inst claims with unique discharge disp) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and unique_discharge_disposition_code = 1
),


usable_discharge_disposition_code as (
select
  '(inst claims with usable discharge disp) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and usable_discharge_disposition_code = 1
),



-- **************************************************
-- diagnosis_code_1 fields: (for institutional claims)
-- **************************************************

missing_diagnosis_code_1_inst as (
select
  '(inst claims with missing dx1) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and missing_diagnosis_code_1 = 1
),


populated_diagnosis_code_1_inst as (
select
  '(inst claims with populated dx1) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and missing_diagnosis_code_1 = 0
),


always_valid_diagnosis_code_1_inst as (
select
  '(inst claims with always valid dx1) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and always_valid_diagnosis_code_1 = 1
),


valid_and_invalid_diagnosis_code_1_inst as (
select
  '(inst claims with valid and invalid dx1) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and valid_and_invalid_diagnosis_code_1 = 1
),


always_invalid_diagnosis_code_1_inst as (
select
  '(inst claims with always invalid dx1) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and always_invalid_diagnosis_code_1 = 1
),


undeterminable_diagnosis_code_1_inst as (
select
  '(inst claims with undeterminable dx1) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and undeterminable_diagnosis_code_1 = 1
),


determinable_diagnosis_code_1_inst as (
select
  '(inst claims with determinable dx1) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and determinable_diagnosis_code_1 = 1
),


unique_diagnosis_code_1_inst as (
select
  '(inst claims with unique dx1) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and unique_diagnosis_code_1 = 1
),


usable_diagnosis_code_1_inst as (
select
  '(inst claims with usable dx1) / (total inst claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_institutional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'institutional'
and usable_diagnosis_code_1 = 1
),


-- **************************************************
-- diagnosis_code_1 fields: (for professional claims)
-- **************************************************

missing_diagnosis_code_1_prof as (
select
  '(prof claims with missing dx1) / (total prof claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_professional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'professional'
and missing_diagnosis_code_1 = 1
),


populated_diagnosis_code_1_prof as (
select
  '(prof claims with populated dx1) / (total prof claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_professional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'professional'
and missing_diagnosis_code_1 = 0
),


always_valid_diagnosis_code_1_prof as (
select
  '(prof claims with always valid dx1) / (total prof claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_professional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'professional'
and always_valid_diagnosis_code_1 = 1
),


valid_and_invalid_diagnosis_code_1_prof as (
select
  '(prof claims with valid and invalid dx1) / (total prof claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_professional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'professional'
and valid_and_invalid_diagnosis_code_1 = 1
),


always_invalid_diagnosis_code_1_prof as (
select
  '(prof claims with always invalid dx1) / (total prof claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_professional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'professional'
and always_invalid_diagnosis_code_1 = 1
),


undeterminable_diagnosis_code_1_prof as (
select
  '(prof claims with undeterminable dx1) / (total prof claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_professional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'professional'
and undeterminable_diagnosis_code_1 = 1
),


determinable_diagnosis_code_1_prof as (
select
  '(prof claims with determinable dx1) / (total prof claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_professional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'professional'
and determinable_diagnosis_code_1 = 1
),


unique_diagnosis_code_1_prof as (
select
  '(prof claims with unique dx1) / (total prof claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_professional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'professional'
and unique_diagnosis_code_1 = 1
),


usable_diagnosis_code_1_prof as (
select
  '(prof claims with usable dx1) / (total prof claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_professional_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where calculated_claim_type = 'professional'
and usable_diagnosis_code_1 = 1
),



-- **************************************************
-- diagnosis_code_1 fields: (for all claims)
-- **************************************************

missing_diagnosis_code_1_all as (
select
  '(claims with missing dx1) / (total claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where missing_diagnosis_code_1 = 1
),


populated_diagnosis_code_1_all as (
select
  '(claims with populated dx1) / (total claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where missing_diagnosis_code_1 = 0
),


always_valid_diagnosis_code_1_all as (
select
  '(claims with always valid dx1) / (total claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where always_valid_diagnosis_code_1 = 1
),


valid_and_invalid_diagnosis_code_1_all as (
select
  '(claims with valid and invalid dx1) / (total claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where valid_and_invalid_diagnosis_code_1 = 1
),


always_invalid_diagnosis_code_1_all as (
select
  '(claims with always invalid dx1) / (total claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where always_invalid_diagnosis_code_1 = 1
),


undeterminable_diagnosis_code_1_all as (
select
  '(claims with undeterminable dx1) / (total claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where undeterminable_diagnosis_code_1 = 1
),


determinable_diagnosis_code_1_all as (
select
  '(claims with determinable dx1) / (total claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where determinable_diagnosis_code_1 = 1
),


unique_diagnosis_code_1_all as (
select
  '(claims with unique dx1) / (total claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where unique_diagnosis_code_1 = 1
),


usable_diagnosis_code_1_all as (
select
  '(claims with usable dx1) / (total claims) * 100' as field,
  cast(count(*) as {{ dbt.type_numeric() }}) / (select * from total_claims) * 100 as field_value
from {{ ref('data_quality__header_values') }}
where usable_diagnosis_code_1 = 1
)

, final as (

  select * from missing_bill_type_code
  union all
  select * from populated_bill_type_code
  union all
  select * from always_valid_bill_type_code
  union all
  select * from valid_and_invalid_bill_type_code
  union all
  select * from always_invalid_bill_type_code
  union all
  select * from undeterminable_bill_type_code
  union all
  select * from determinable_bill_type_code
  union all
  select * from unique_bill_type_code
  union all
  select * from usable_bill_type_code

  union all

  select * from missing_drg_code
  union all
  select * from populated_drg_code
  union all
  select * from always_valid_drg_code
  union all
  select * from valid_and_invalid_drg_code
  union all
  select * from always_invalid_drg_code
  union all
  select * from undeterminable_drg_code
  union all
  select * from determinable_drg_code
  union all
  select * from unique_drg_code
  union all
  select * from usable_drg_code

  union all

  select * from missing_admit_type_code
  union all
  select * from populated_admit_type_code
  union all
  select * from always_valid_admit_type_code
  union all
  select * from valid_and_invalid_admit_type_code
  union all
  select * from always_invalid_admit_type_code
  union all
  select * from undeterminable_admit_type_code
  union all
  select * from determinable_admit_type_code
  union all
  select * from unique_admit_type_code
  union all
  select * from usable_admit_type_code

  union all

  select * from missing_admit_source_code
  union all
  select * from populated_admit_source_code
  union all
  select * from always_valid_admit_source_code
  union all
  select * from valid_and_invalid_admit_source_code
  union all
  select * from always_invalid_admit_source_code
  union all
  select * from undeterminable_admit_source_code
  union all
  select * from determinable_admit_source_code
  union all
  select * from unique_admit_source_code
  union all
  select * from usable_admit_source_code

  union all

  select * from missing_discharge_disposition_code
  union all
  select * from populated_discharge_disposition_code
  union all
  select * from always_valid_discharge_disposition_code
  union all
  select * from valid_and_invalid_discharge_disposition_code
  union all
  select * from always_invalid_discharge_disposition_code
  union all
  select * from undeterminable_discharge_disposition_code
  union all
  select * from determinable_discharge_disposition_code
  union all
  select * from unique_discharge_disposition_code
  union all
  select * from usable_discharge_disposition_code

  union all

  select * from missing_diagnosis_code_1_inst
  union all
  select * from populated_diagnosis_code_1_inst
  union all
  select * from always_valid_diagnosis_code_1_inst
  union all
  select * from valid_and_invalid_diagnosis_code_1_inst
  union all
  select * from always_invalid_diagnosis_code_1_inst
  union all
  select * from undeterminable_diagnosis_code_1_inst
  union all
  select * from determinable_diagnosis_code_1_inst
  union all
  select * from unique_diagnosis_code_1_inst
  union all
  select * from usable_diagnosis_code_1_inst

  union all

  select * from missing_diagnosis_code_1_prof
  union all
  select * from populated_diagnosis_code_1_prof
  union all
  select * from always_valid_diagnosis_code_1_prof
  union all
  select * from valid_and_invalid_diagnosis_code_1_prof
  union all
  select * from always_invalid_diagnosis_code_1_prof
  union all
  select * from undeterminable_diagnosis_code_1_prof
  union all
  select * from determinable_diagnosis_code_1_prof
  union all
  select * from unique_diagnosis_code_1_prof
  union all
  select * from usable_diagnosis_code_1_prof

  union all

  select * from missing_diagnosis_code_1_all
  union all
  select * from populated_diagnosis_code_1_all
  union all
  select * from always_valid_diagnosis_code_1_all
  union all
  select * from valid_and_invalid_diagnosis_code_1_all
  union all
  select * from always_invalid_diagnosis_code_1_all
  union all
  select * from undeterminable_diagnosis_code_1_all
  union all
  select * from determinable_diagnosis_code_1_all
  union all
  select * from unique_diagnosis_code_1_all
  union all
  select * from usable_diagnosis_code_1_all

)

select
      field
    , field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from final
