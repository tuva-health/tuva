{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


with claim_dates as(
    select
         {{ dbt.concat([
            dbt.safe_cast("claim_id", api.Column.translate_type("string")),
            "'-'",
            dbt.safe_cast("claim_line_number", api.Column.translate_type("string"))
            ]) }} as medical_claim_id
        , patient_id
        , payer
        {% if target.type == 'fabric' %}
            , "plan"
        {% else %}
            , plan
        {% endif %}
        , coalesce(claim_line_start_date, claim_start_date, admission_date) as inferred_claim_start_date
        , coalesce(claim_line_end_date, claim_end_date, discharge_date) as inferred_claim_end_date
        , case
            when claim_line_start_date is not null then 'claim_line_start_date'
            when claim_line_start_date is null and claim_start_date is not null then 'claim_start_date'
            when claim_line_start_date is null and claim_start_date is null and admission_date is not null then 'admission_date'
        end as inferred_claim_start_column_used
        , case
            when claim_line_end_date is not null then 'claim_line_end_date'
            when claim_line_end_date is null and claim_end_date is not null then 'claim_end_date'
            when claim_line_end_date is null and claim_end_date is null and discharge_date is not null then 'discharge_date'
        end as inferred_claim_end_column_used
    from {{ ref('normalized_input__medical_claim') }}
)

, claim_year_month as(
    select
        medical_claim_id
        , patient_id
        , payer
        {% if target.type == 'fabric' %}
            , "plan"
        {% else %}
            , plan
        {% endif %}
        , inferred_claim_start_date
        , inferred_claim_end_date
        , inferred_claim_start_column_used
        , inferred_claim_end_column_used

        {% set year_part = date_part('year', 'inferred_claim_start_date') %}
        {% set month_part = date_part('month', 'inferred_claim_start_date') %}

        , {{ dbt.concat([
                dbt.safe_cast(year_part, api.Column.translate_type("string")),
                dbt.right(
                    dbt.concat([
                        "'0'",
                        dbt.safe_cast(month_part, api.Column.translate_type("string")),
                    ]),
                    2
                )
            ]) }} as inferred_claim_start_year_month

    {% set year_part = date_part('year', 'inferred_claim_end_date') %}
    {% set month_part = date_part('month', 'inferred_claim_end_date') %}

    , {{ dbt.concat([
            dbt.safe_cast(year_part, api.Column.translate_type("string")),
            dbt.right(
                dbt.concat([
                    "'0'",
                    dbt.safe_cast(month_part, api.Column.translate_type("string")),
                ]),
                2
            )
        ]) }} as inferred_claim_end_year_month

from claim_dates

)

select distinct
    claim.medical_claim_id
    , claim.patient_id
    , claim.payer
    {% if target.type == 'fabric' %}
        , claim."plan"
    {% else %}
        , claim.plan
    {% endif %}
    , claim.inferred_claim_start_year_month
    , claim.inferred_claim_end_year_month
    , claim.inferred_claim_start_column_used
    , claim.inferred_claim_end_column_used
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run
from {{ ref('core__member_months')}} mm
inner join claim_year_month claim
    on mm.patient_id = claim.patient_id
    and mm.payer = claim.payer
    {% if target.type == 'fabric' %}
        and mm."plan" = claim."plan"
    {% else %}
        and mm.plan = claim.plan
    {% endif %}
    and mm.year_month >= claim.inferred_claim_start_year_month
    and mm.year_month <= claim.inferred_claim_end_year_month