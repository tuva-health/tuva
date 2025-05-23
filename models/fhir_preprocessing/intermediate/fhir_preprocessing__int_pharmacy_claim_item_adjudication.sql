{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}
with adjudication_amount as (

    select
          claim_id
        , claim_line_number
        , 'ADJ_TYPE' as eob_item_adjudication_category_system
        , 'benefit' as eob_item_adjudication_category_code
        , 'USD' as eob_item_adjudication_amount_currency
        /* required by HEDIS, cannot be <= $0 */
        , case
            when paid_amount <= 0 then cast(0.01 as {{ dbt.type_numeric() }} )
            else cast(paid_amount as {{ dbt.type_numeric() }} )
          end as eob_item_adjudication_amount_value
    from {{ ref('fhir_preprocessing__stg_core__pharmacy_claim') }}

)

, adjudication_status as (
    select
          claim_id
        , claim_line_number
        , 'ADJ_STATUS' as eob_item_adjudication_category_system
        , case
            when in_network_flag = 1 then 'innetwork'
            when in_network_flag = 0 then 'outofnetwork'
            else 'other'
          end as eob_item_adjudication_category_code
        , 'USD' as eob_item_adjudication_amount_currency
        /* required by HEDIS, cannot be <= $0 */
        , case
            when paid_amount <= 0 then cast(0.01 as {{ dbt.type_numeric() }} )
            else cast(paid_amount as {{ dbt.type_numeric() }} )
          end as eob_item_adjudication_amount_value
    from {{ ref('fhir_preprocessing__stg_core__pharmacy_claim') }}
)

, unioned as (

    select * from adjudication_amount
    union all
    select * from adjudication_status

)

/* create a json string for CSV export */
{{ create_json_object(
    table_ref='unioned',
    group_by_col='claim_id, claim_line_number',
    object_col_name='eob_item_adjudication_list',
    object_col_list=[
        'eob_item_adjudication_category_system'
        , 'eob_item_adjudication_category_code'
        , 'eob_item_adjudication_amount_currency'
        , 'eob_item_adjudication_amount_value'
    ]
) }}
