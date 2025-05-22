{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}
with adjudication as (

    select
          claim_id
        , claim_line_number
        , eob_item_adjudication_list
    from {{ ref('fhir_preprocessing__int_medical_claim_item_adjudication') }}

)

, modifier as (

    select
          claim_id
        , claim_line_number
        , eob_item_modifier_list
    from {{ ref('fhir_preprocessing__int_medical_claim_item_modifier') }}

)

, joined as (

    select
          medical_claim.claim_id
        /* required for FHIR validation, sequence must be >0, temporary fix for possible issues with ADR  */
        , abs(medical_claim.claim_line_number) as eob_item_sequence
        , medical_claim.revenue_center_code as eob_item_revenue_code
        , medical_claim.revenue_center_description as eob_item_revenue_display
        , case
            when medical_claim.hcpcs_code is not null then 'CPT'
            else null
          end as eob_item_product_or_service_system
        /* required for FHIR validation, default to dummy code */
        , coalesce(medical_claim.hcpcs_code, '00000') as eob_item_product_or_service_code
        , cast(coalesce(
              medical_claim.claim_line_start_date
            , medical_claim.claim_start_date
          ) as {{ dbt.type_string() }} ) as eob_item_serviced_date /* cast date to string for redshift support */
        , case
            when medical_claim.place_of_service_code is not null then 'POS'
            else null
          end as eob_item_location_system
        , medical_claim.place_of_service_code as eob_item_location_code
        , medical_claim.place_of_service_description as eob_item_location_display
        , adjudication.eob_item_adjudication_list
        , modifier.eob_item_modifier_list
    from {{ ref('fhir_preprocessing__stg_core__medical_claim') }} as medical_claim
        left outer join adjudication
            on medical_claim.claim_id = adjudication.claim_id
            and medical_claim.claim_line_number = adjudication.claim_line_number
        left outer join modifier
            on medical_claim.claim_id = modifier.claim_id
            and medical_claim.claim_line_number = modifier.claim_line_number

)

/* create a json string for CSV export */
{{ create_json_object(
    table_ref='joined',
    group_by_col='claim_id',
    object_col_name='eob_item_list',
    object_col_list=[
        'eob_item_sequence'
        , 'eob_item_revenue_code'
        , 'eob_item_revenue_display'
        , 'eob_item_product_or_service_system'
        , 'eob_item_product_or_service_code'
        , 'eob_item_serviced_date'
        , 'eob_item_location_system'
        , 'eob_item_location_code'
        , 'eob_item_location_display'
        , 'eob_item_adjudication_list'
        , 'eob_item_modifier_list'
    ]
) }}
