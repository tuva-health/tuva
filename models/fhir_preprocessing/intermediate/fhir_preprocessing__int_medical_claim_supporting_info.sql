{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with admission_period as (

    select
          claim_id
        , 'admissionperiod' as eob_supporting_info_category_code
        , null as eob_supporting_info_code /* required for union */
        , null as eob_supporting_info_system /* required for union */
        , admission_date as eob_supporting_info_timing_start
        , discharge_date as eob_supporting_info_timing_end
    from {{ ref('fhir_preprocessing__stg_core__medical_claim') }}
    where claim_line_number = 1 /* filter to claim header */
    and admission_date is not null

)

, type_of_bill as (

    select
          claim_id
        , 'typeofbill' as eob_supporting_info_category_code
        , bill_type_code as eob_supporting_info_code
        , 'UBTOB' as eob_supporting_info_system
        , null as eob_supporting_info_timing_start /* required for union */
        , null as eob_supporting_info_timing_end /* required for union */
    from {{ ref('fhir_preprocessing__stg_core__medical_claim') }}
    where claim_line_number = 1 /* filter to claim header */
    and bill_type_code is not null

)

, unioned as (

    select * from admission_period
    union all
    select * from type_of_bill

)

, add_sequence as (

    select
          claim_id
        , eob_supporting_info_category_code
        , eob_supporting_info_code
        , eob_supporting_info_system
        , eob_supporting_info_timing_start
        , eob_supporting_info_timing_end
        , row_number() over(
            partition by claim_id
            order by eob_supporting_info_category_code
          ) as eob_supporting_info_sequence
    from unioned

)

/* create a json string for CSV export */
{{ create_json_object(
    table_ref='add_sequence',
    group_by_col='claim_id',
    order_by_col='eob_supporting_info_sequence',
    object_col_name='eob_supporting_info_list',
    object_col_list=[
        'eob_supporting_info_sequence'
        , 'eob_supporting_info_category_code'
        , 'eob_supporting_info_code'
        , 'eob_supporting_info_system'
        , 'eob_supporting_info_timing_start'
        , 'eob_supporting_info_timing_end'
    ]
) }}
