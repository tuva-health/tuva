{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with professional_denominator as(
  select 
    cast('professional' as {{ dbt.type_string() }} ) as test_denominator_name
    , cast(count(distinct claim_id) as int) as denominator
    , '{{ var('tuva_last_run')}}' as tuva_last_run
  from {{ ref('medical_claim') }}
  where claim_type = 'professional'
)

, institutional_denominator as(
  select 
     cast('institutional' as {{ dbt.type_string() }} ) as test_denominator_name
    , count(distinct claim_id) as denominator
    , '{{ var('tuva_last_run')}}' as tuva_last_run
  from {{ ref('medical_claim') }}
  where claim_type = 'institutional'
)

, all_claim_denominator as(
  select 
    cast('all' as {{ dbt.type_string() }} ) as test_denominator_name
    , count(distinct claim_id) as denominator
    , '{{ var('tuva_last_run')}}' as tuva_last_run
  from {{ ref('medical_claim') }}
  where claim_type is not null
)

, bill_type_denominator as(
    select
        cast('bill_type_code invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where bill_type_code is not null
)

, revenue_center_denominator as(
    select
        cast('revenue_center_code invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where revenue_center_code is not null
)

, discharge_disposition_denominator as(
    select
        cast('discharge_disposition_code invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where discharge_disposition_code is not null
)
, admit_source_denominator as( 
    select
        cast('admit_source_code invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where admit_source_code is not null
)

, admit_type_denominator as(
    select
        cast('admit_type_code invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where admit_type_code is not null
)

, ms_drg_denominator as(
    select
        cast('ms_drg_code invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where ms_drg_code is not null
)
, present_on_admission_denominator as(
    select
        cast('diagnosis_poa_1 invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where diagnosis_poa_1 is not null
)

, procedure_code_type_denominator as(
    select
        cast('procedure_code_type invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where procedure_code_type is not null
)
, place_of_service_denominator as(
    select
        cast('place_of_service_code invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where place_of_service_code is not null
)
, diagnosis_code_type_denominator as(
    select
        cast('diagnosis_code_type invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where diagnosis_code_type is not null
)

, diagnosis_code_denominator as(
    select
        cast('diagnosis_code_1 invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where diagnosis_code_1 is not null

)

, claim_type_denominator as(
    select
        cast('claim_type invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where claim_type is not null
)

select * from institutional_denominator
union all 
select * from professional_denominator
union all
select * from bill_type_denominator
union all
select * from revenue_center_denominator
union all
select * from discharge_disposition_denominator
union all
select * from admit_source_denominator
union all
select * from admit_type_denominator
union all
select * from ms_drg_denominator
union all
select * from present_on_admission_denominator
union all
select * from procedure_code_type_denominator
union all
select * from place_of_service_denominator
union all
select * from diagnosis_code_type_denominator
union all
select * from diagnosis_code_denominator
union all
select * from claim_type_denominator
