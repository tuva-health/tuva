with month_dates as (
    select
         year_month
       , convert(varchar(8), min(full_date), 112) as month_start_date
       , convert(varchar(8), max(full_date), 112) as month_end_date
    from {{ ref('reference_data__calendar') }}
    group by year_month
),

final_with_sk as (
    select
         mm.*
       , c.contract_id
    from {{ ref('core__member_months') }} mm
    inner join {{ ref('epic__contract_id') }} c
        on mm.data_source = c.data_source
       and mm.payer = c.payer
       and mm.[plan] = c.[plan]
),

attribution as (
    select
         cast(fws.person_id as varchar(50)) as person_id
       , 0 as adjustment_type_code
       , 0 as adjustment_sequence
       , md.month_start_date as start_date
       , md.month_end_date as end_date
       , cast(coalesce(fws.payer_attributed_provider, fws.custom_attributed_provider) as varchar(10)) as attributed_provider_npi
       , cast(null as varchar(10)) as attributed_provider_nucc_taxonomy_code
       , cast(null as int) as attributed_provider_specialty_code_set
       , cast(null as varchar(10)) as attributed_provider_specialty_code
       , cast('1' as varchar(50)) as contract_id
    from final_with_sk fws
    left join month_dates md
        on fws.year_month = md.year_month
)

select 
         person_id as [Person ID]
       , adjustment_type_code as [Adjustment Type Code]
       , adjustment_sequence as [Adjustment Sequence]
       , start_date as [Start Date]
       , end_date as [End Date]
       , attributed_provider_npi as [Attributed Provider NPI]
       , attributed_provider_nucc_taxonomy_code as [Attributed Provider NUCC Taxonomy Code]
       , attributed_provider_specialty_code_set as [Attributed Provider Specialty Code Set]
       , attributed_provider_specialty_code as [Attributed Provider Specialty Code]
       , contract_id as [Contract ID]
from attribution
