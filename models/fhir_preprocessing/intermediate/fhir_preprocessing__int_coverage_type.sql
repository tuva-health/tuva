{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}
with coverage_staging as (

    select
          patient_internal_id
        , resource_internal_id
        , coverage_type_product
    from {{ ref('fhir_preprocessing__int_coverage') }}

)

, product_type as (

    select
          patient_internal_id
        , resource_internal_id
        , 'COVERAGE_TYPE' as coverage_type_system
        , coverage_type_product as coverage_type_code
    from coverage_staging

)

/* Map to standardized codes for benefit type */
, medical_benefit as (

    select
           patient_internal_id
         , resource_internal_id
         , 'ACT_CODE' as coverage_type_system
         , case
            when coverage_type_product in (
                  'PPO'
                , 'POS'
                , 'CEP'
                , 'HMO'
                , 'MMO'
                , 'MOS'
                , 'MPO'
                , 'MEP'
            ) then 'MCPOL'
            when coverage_type_product in (
                  'MCR'
                , 'MP'
                , 'MC'
                , 'MCS'
                , 'MMP'
                , 'MDE'
            ) then 'RETIRE'
            else 'SUBSIDIZ'
          end as coverage_type_code
    from coverage_staging

)

, pharmacy_benefit as (

    select distinct
          patient_internal_id
        , coverage_internal_id as resource_internal_id
        , 'ACT_CODE' as coverage_type_system
        , 'DRUGPOL' as coverage_type_code
    from {{ ref('fhir_preprocessing__int_pharmacy_claim_eob') }}

)

, unioned as (
    select * from product_type
    union all
    select * from medical_benefit
    union all
    select * from pharmacy_benefit
)

/* create a json string for CSV export */
{{ create_json_object(
    table_ref='unioned',
    group_by_col='patient_internal_id, resource_internal_id',
    object_col_name='coverage_type_list',
    object_col_list=[
        'coverage_type_system'
        , 'coverage_type_code'
    ]
) }}
