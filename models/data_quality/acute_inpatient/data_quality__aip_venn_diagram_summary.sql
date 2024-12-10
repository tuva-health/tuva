{{ config(
    enabled = var('claims_enabled', False)
) }}

with rb as (

    select 
        count(*) 
    from {{ ref('data_quality__aip_venn_diagram') }} 
    where rb = 1 and drg = 0 and bill = 0

)

, drg as (

    select 
        count(*) 
    from {{ ref('data_quality__aip_venn_diagram') }} 
    where rb = 0 and drg = 1 and bill = 0

)

, bill as (

    select 
        count(*) 
    from {{ ref('data_quality__aip_venn_diagram') }} 
    where rb = 0 and drg = 0 and bill = 1

)

, rb_drg as (

    select 
        count(*) 
    from {{ ref('data_quality__aip_venn_diagram') }} 
    where rb = 1 and drg = 1 and bill = 0

)

, rb_bill as (

    select 
        count(*) 
    from {{ ref('data_quality__aip_venn_diagram') }} 
    where rb = 1 and drg = 0 and bill = 1

)

, drg_bill as (

    select 
        count(*) 
    from {{ ref('data_quality__aip_venn_diagram') }} 
    where rb = 0 and drg = 1 and bill = 1

)

, rb_drg_bill as (

    select 
        count(*) 
    from {{ ref('data_quality__aip_venn_diagram') }} 
    where rb = 1 and drg = 1 and bill = 1

)

, summary_cte as (

    select
          1 as index
        , 'rb' as venn_section
        , (select * from rb) as claims
        , round(
            (select * from rb) * 100.0 / 
            (
                select 
                    total_claims 
                from {{ ref('data_quality__calculated_claim_type_percentages') }}
                where calculated_claim_type = 'institutional'
            )
        , 1) as percent_of_institutional_claims

    union all

    select
          2 as index
        , 'drg' as venn_section
        , (select * from drg) as claims
        , round(
            (select * from drg) * 100.0 / 
            (
                select 
                    total_claims 
                from {{ ref('data_quality__calculated_claim_type_percentages') }}
                where calculated_claim_type = 'institutional'
            )
        , 1) as percent_of_institutional_claims

    union all

    select
          3 as index
        , 'bill' as venn_section
        , (select * from bill) as claims
        , round(
            (select * from bill) * 100.0 / 
            (
                select 
                    total_claims 
                from {{ ref('data_quality__calculated_claim_type_percentages') }}
                where calculated_claim_type = 'institutional'
            )
        , 1) as percent_of_institutional_claims

    union all

    select
          4 as index
        , 'rb_drg' as venn_section
        , (select * from rb_drg) as claims
        , round(
            (select * from rb_drg) * 100.0 / 
            (
                select 
                    total_claims 
                from {{ ref('data_quality__calculated_claim_type_percentages') }}
                where calculated_claim_type = 'institutional'
            )
        , 1) as percent_of_institutional_claims

    union all

    select
          5 as index
        , 'rb_bill' as venn_section
        , (select * from rb_bill) as claims
        , round(
            (select * from rb_bill) * 100.0 / 
            (
                select 
                    total_claims 
                from {{ ref('data_quality__calculated_claim_type_percentages') }}
                where calculated_claim_type = 'institutional'
            )
        , 1) as percent_of_institutional_claims

    union all

    select
          6 as index
        , 'drg_bill' as venn_section
        , (select * from drg_bill) as claims
        , round(
            (select * from drg_bill) * 100.0 / 
            (
                select 
                    total_claims 
                from {{ ref('data_quality__calculated_claim_type_percentages') }}
                where calculated_claim_type = 'institutional'
            )
        , 1) as percent_of_institutional_claims

    union all

    select
          7 as index
        , 'rb_drg_bill' as venn_section
        , (select * from rb_drg_bill) as claims
        , round(
            (select * from rb_drg_bill) * 100.0 / 
            (
                select 
                    total_claims 
                from {{ ref('data_quality__calculated_claim_type_percentages') }}
                where calculated_claim_type = 'institutional'
            )
        , 1) as percent_of_institutional_claims

)

select 
    * 
from summary_cte





