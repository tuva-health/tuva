{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
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
            when paid_amount <= 0 then cast(0.01 as {{ dbt.type_string() }} ) /* cast as string for union */
            else cast(paid_amount as {{ dbt.type_string() }} ) /* cast as string for union */
          end as eob_item_adjudication_amount_value
    from {{ ref('fhir_preprocessing__stg_core__medical_claim') }}

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
        , null as eob_item_adjudication_amount_currency /* required for union */
        , null as eob_item_adjudication_amount_value /* required for union */
    from {{ ref('fhir_preprocessing__stg_core__medical_claim') }}
)

, unioned as (

    select * from adjudication_amount
    union all
    select * from adjudication_status

)

/* create a json string for CSV export */
select
      claim_id
    , claim_line_number
    , to_json(
        array_agg(
            object_construct(
                  'eob_item_adjudication_category_system', eob_item_adjudication_category_system
                , 'eob_item_adjudication_category_code', eob_item_adjudication_category_code
                , 'eob_item_adjudication_amount_currency', eob_item_adjudication_amount_currency
                , 'eob_item_adjudication_amount_value', cast(eob_item_adjudication_amount_value as {{ dbt.type_numeric() }} )
            )
        )
      ) as eob_item_adjudication_list
from unioned
group by
      claim_id
    , claim_line_number
