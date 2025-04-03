{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


with claim_dates as(
    select
        claim_id
        , claim_line_number
        , person_id
        , member_id
        , payer
        , {{ quote_column('plan') }}
        , data_source
        , {{ concat_custom([
                date_part('year', 'paid_date'),
                dbt.right(
                    concat_custom([
                        "'0'",
                        date_part('month', 'paid_date'),
                    ]),
                    2
                )
            ]) }} as paid_year_month
    from {{ ref('normalized_input__pharmacy_claim') }}
)

select distinct
     claim.claim_id
    , claim.claim_line_number
    , claim.person_id
    , claim.member_id
    , claim.payer
    , claim.{{ quote_column('plan') }}
    , claim.data_source
    , mm.member_month_key
    , claim.paid_year_month
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run
from {{ ref('core__member_months')}} mm
inner join claim_dates claim
    on mm.person_id = claim.person_id
    and mm.member_id = claim.member_id
    and mm.payer = claim.payer
    and mm.{{ quote_column('plan') }} = claim.{{ quote_column('plan') }}
    and mm.year_month = claim.paid_year_month
