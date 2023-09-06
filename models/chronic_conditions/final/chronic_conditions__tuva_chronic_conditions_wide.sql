{{ config(
     enabled = var('tuva_chronic_conditions_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with condition_columns as (

    select distinct
          condition
        , condition_column_name
    from {{ ref('chronic_conditions__tuva_chronic_conditions_hierarchy') }}

)

select
      p.patient_id
    , {{ dbt_utils.pivot(
          column='cc.condition_column_name'
        , values=dbt_utils.get_column_values(
              ref('chronic_conditions__tuva_chronic_conditions_hierarchy')
            , 'condition_column_name'
            ,'condition_column_name'
          )
        , agg='max'
        , then_value= 1
        , else_value= 0
        , quote_identifiers = False
      ) }}
      , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('tuva_chronic_conditions__stg_core__patient') }} p
     left join {{ ref('chronic_conditions__tuva_chronic_conditions_long') }} l
        on p.patient_id = l.patient_id
     left join condition_columns cc
        on l.condition = cc.condition
group by
    p.patient_id