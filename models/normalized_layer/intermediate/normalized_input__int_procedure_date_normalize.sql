{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}


with pivot_procedure as (
    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_1' as procedure_column
        , procedure_date_1 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_2' as procedure_column
        , procedure_date_2 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_3' as procedure_column
        , procedure_date_3 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_4' as procedure_column
        , procedure_date_4 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_5' as procedure_column
        , procedure_date_5 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_6' as procedure_column
        , procedure_date_6 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_7' as procedure_column
        , procedure_date_7 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_8' as procedure_column
        , procedure_date_8 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_9' as procedure_column
        , procedure_date_9 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_10' as procedure_column
        , procedure_date_10 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_11' as procedure_column
        , procedure_date_11 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_12' as procedure_column
        , procedure_date_12 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_13' as procedure_column
        , procedure_date_13 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_14' as procedure_column
        , procedure_date_14 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_15' as procedure_column
        , procedure_date_15 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_16' as procedure_column
        , procedure_date_16 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_17' as procedure_column
        , procedure_date_17 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_18' as procedure_column
        , procedure_date_18 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_19' as procedure_column
        , procedure_date_19 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_20' as procedure_column
        , procedure_date_20 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_21' as procedure_column
        , procedure_date_21 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_22' as procedure_column
        , procedure_date_22 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_23' as procedure_column
        , procedure_date_23 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_24' as procedure_column
        , procedure_date_24 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'procedure_date_25' as procedure_column
        , procedure_date_25 as procedure_date
    from {{ ref('normalized_input__stg_medical_claim') }}
)

select
    claim_id
    , data_source
    , procedure_column
    , procedure_date
    , count(*) as procedure_date_occurrence_count
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from pivot_procedure as piv
where claim_type = 'institutional'
group by
    claim_id
    , data_source
    , procedure_column
    , procedure_date
