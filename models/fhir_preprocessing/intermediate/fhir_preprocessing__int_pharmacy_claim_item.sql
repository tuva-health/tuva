{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
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
        , pharmacy_claim.claim_line_number as eob_item_sequence
        , 'NDC' as eob_item_product_or_service_system
        , pharmacy_claim.ndc_code as eob_item_product_or_service_code
        , coalesce(
              pharmacy_claim.dispensing_date
            , pharmacy_claim.paid_date
          ) as eob_item_serviced_date
        , adjudication.eob_item_adjudication_list
    from {{ ref('fhir_preprocessing__stg_core__pharmacy_claim') }} as pharmacy_claim
        left outer join adjudication
            on pharmacy_claim.claim_id = adjudication.claim_id
            and pharmacy_claim.claim_line_number = adjudication.claim_line_number

)

/* create a json string for CSV export */
select
      claim_id
    , to_json(
        array_agg(
            object_construct(
                  'eob_item_sequence', eob_item_sequence
                , 'eob_item_product_or_service_system', eob_item_product_or_service_system
                , 'eob_item_product_or_service_code', eob_item_product_or_service_code
                , 'eob_item_serviced_date', eob_item_serviced_date
                , 'eob_item_adjudication_list', eob_item_adjudication_list
            )
        ) within group (order by eob_item_sequence)
      ) as eob_item_list
from joined
group by claim_id
