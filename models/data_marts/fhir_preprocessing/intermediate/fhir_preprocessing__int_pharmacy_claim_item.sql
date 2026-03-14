{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}
with adjudication as (

    select
          claim_id
        , claim_line_number
        , eob_item_adjudication_list
    from {{ ref('fhir_preprocessing__int_pharmacy_claim_item_adjudication') }}

)

, joined as (

    select
          pharmacy_claim.claim_id
        /* required for FHIR validation, sequence must be >0, temporary fix for possible issues with ADR  */
        , abs(pharmacy_claim.claim_line_number) as eob_item_sequence
        , cast('NDC' as {{ dbt.type_string() }} ) as eob_item_product_or_service_system
        , pharmacy_claim.ndc_code as eob_item_product_or_service_code
        , cast(coalesce(
              pharmacy_claim.dispensing_date
            , pharmacy_claim.paid_date
          ) as {{ dbt.type_string() }} ) as eob_item_serviced_date /* cast date to string for redshift support */
        , adjudication.eob_item_adjudication_list
    from {{ ref('fhir_preprocessing__stg_core__pharmacy_claim') }} as pharmacy_claim
        left outer join adjudication
            on pharmacy_claim.claim_id = adjudication.claim_id
            and pharmacy_claim.claim_line_number = adjudication.claim_line_number
    where pharmacy_claim.ndc_code is not null

)

/* create a json string for CSV export */
{{ the_tuva_project.create_json_object(
    table_ref='joined',
    group_by_col='claim_id',
    object_col_name='eob_item_list',
    object_col_list=[
        'eob_item_sequence'
        , 'eob_item_product_or_service_system'
        , 'eob_item_product_or_service_code'
        , 'eob_item_serviced_date'
        , 'eob_item_adjudication_list'
    ]
) }}
