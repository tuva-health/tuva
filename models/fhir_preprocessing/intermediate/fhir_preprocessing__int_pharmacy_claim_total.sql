{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with total_amount as (

    select
          claim_id
        , 'ADJ_TYPE' as eob_total_category_system
        , 'benefit' as eob_total_category_code
        , 'USD' as eob_total_amount_currency
        /* required by HEDIS, cannot be <= $0 */
        , case when paid_amount <= 0 then cast(0.01 as {{ dbt.type_numeric() }} )
            else cast(paid_amount as {{ dbt.type_numeric() }} )
          end as eob_total_amount_value
    from {{ ref('fhir_preprocessing__stg_core__pharmacy_claim') }}
    where claim_line_number = 1 /* filter to claim header */

)

, total_status as (

    select
          claim_id
        , 'ADJ_STATUS' as eob_total_category_system
        , case
            when in_network_flag = 1 then 'innetwork'
            when in_network_flag = 0 then 'outofnetwork'
            else 'other'
          end as eob_total_category_code
        , 'USD' as eob_total_amount_currency
        /* required by HEDIS, cannot be <= $0 */
        , case when paid_amount <= 0 then cast(0.01 as {{ dbt.type_numeric() }} )
            else cast(paid_amount as {{ dbt.type_numeric() }} )
          end as eob_total_amount_value
    from {{ ref('fhir_preprocessing__stg_core__pharmacy_claim') }}
    where claim_line_number = 1 /* filter to claim header */

)

, unioned as (

    select * from total_amount
    union all
    select * from total_status

)

/* create a json string for CSV export */
{{ create_json_object(
    table_ref='unioned',
    group_by_col='claim_id',
    object_col_name='eob_total_list',
    object_col_list=[
        'eob_total_category_system'
        , 'eob_total_category_code'
        , 'eob_total_amount_currency'
        , 'eob_total_amount_value'
    ]
) }}
