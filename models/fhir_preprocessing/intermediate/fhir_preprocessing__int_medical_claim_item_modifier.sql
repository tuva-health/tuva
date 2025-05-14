{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

/* unpivot hcpcs modifier codes into rows to be grouped into a json list for CSV export */

with hcpcs_modifier_1 as (

    select
          claim_id
        , claim_line_number
        , 'CPT' as eob_item_modifier_system
        , hcpcs_modifier_1 as eob_item_modifier_code
    from {{ ref('fhir_preprocessing__stg_core__medical_claim') }}
    where hcpcs_modifier_1 is not null

)

, hcpcs_modifier_2 as (

    select
          claim_id
        , claim_line_number
        , 'CPT' as eob_item_modifier_system
        , hcpcs_modifier_2 as eob_item_modifier_code
    from {{ ref('fhir_preprocessing__stg_core__medical_claim') }}
    where hcpcs_modifier_2 is not null

)

, hcpcs_modifier_3 as (

    select
          claim_id
        , claim_line_number
        , 'CPT' as eob_item_modifier_system
        , hcpcs_modifier_3 as eob_item_modifier_code
    from {{ ref('fhir_preprocessing__stg_core__medical_claim') }}
    where hcpcs_modifier_3 is not null

)

, hcpcs_modifier_4 as (

    select
          claim_id
        , claim_line_number
        , 'CPT' as eob_item_modifier_system
        , hcpcs_modifier_4 as eob_item_modifier_code
    from {{ ref('fhir_preprocessing__stg_core__medical_claim') }}
    where hcpcs_modifier_4 is not null

)

, hcpcs_modifier_5 as (

    select
          claim_id
        , claim_line_number
        , 'CPT' as eob_item_modifier_system
        , hcpcs_modifier_5 as eob_item_modifier_code
    from {{ ref('fhir_preprocessing__stg_core__medical_claim') }}
    where hcpcs_modifier_5 is not null

)

, unioned as (

    select * from hcpcs_modifier_1
    union all
    select * from hcpcs_modifier_2
    union all
    select * from hcpcs_modifier_3
    union all
    select * from hcpcs_modifier_4
    union all
    select * from hcpcs_modifier_5

)

/* create a json string for CSV export */
{{ create_json_object(
    table_ref='unioned',
    group_by_col='claim_id, claim_line_number',
    order_by_col=none,
    object_col_name='eob_item_modifier_list',
    object_col_list=[
        'eob_item_modifier_system'
        , 'eob_item_modifier_code'
    ]
) }}
