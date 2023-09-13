{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
    cast({{ date_part("year", "recorded_date") }} as {{ dbt.type_string() }}) as recorded_date_year
    , count(distinct(code)) as unique_codes_in_ccs_count
    , count(distinct(case when ed_classification_capture = 0 then code else null end)) as unique_unclassified_codes_in_ccs_count
    , count(code) as condition_row_count
    , (1 - avg(ed_classification_capture)) * 100 as condition_row_unclassified_percent
    , sum(case when ed_classification_capture = 0 then claim_paid_amount_sum else 0 end) as unclassified_claim_paid_amount_sum

from {{ ref('ed_classification__int_merge_condition') }}
group by recorded_date_year
order by unclassified_claim_paid_amount_sum desc
