{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with service_category_1_mapping as (
    select distinct
        a.claim_id
        , a.claim_line_number
        , a.data_source
        , a.claim_type
        , case when s.service_category_1 is null and b.service_category_1 is not null then 'Service cat value not in seed table'
            else b.service_category_1
          end as service_category_1
        , case when s.service_category_2 is null and b.service_category_2 is not null then 'Service cat value not in seed table'
            else s.service_category_2
          end as service_category_2
        , case
            when s.service_category_3 is null and b.service_category_3 is not null then 'Service cat value not in seed table'
            else s.service_category_3
          end as service_category_3
        , b.service_category_2 as original_service_cat_2
        , b.service_category_3 as original_service_cat_3
        , s.priority
        , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
        , b.source_model_name
    from {{ ref('service_category_grouper__stg_medical_claim') }} as a
    left outer join {{ ref('service_category_grouper__combined_professional') }} as b
      on a.claim_id = b.claim_id
      and a.claim_line_number = b.claim_line_number
      and a.data_source = b.data_source
    left outer join {{ ref('service_category_grouper__service_categories') }} as s
      on b.service_category_1 = s.service_category_1
      and b.service_category_2 = s.service_category_2
      and b.service_category_3 = s.service_category_3
    where a.claim_type = 'professional'

    union all

    select distinct
        a.claim_id
        , a.claim_line_number
        , a.data_source
        , a.claim_type
        , case when s.service_category_1 is null and b.service_category_1 is not null then 'Service cat value not in seed table'
            else b.service_category_1
          end as service_category_1
        , case when s.service_category_2 is null and b.service_category_2 is not null then 'Service cat value not in seed table'
            else s.service_category_2
          end as service_category_2
        , case
            when s.service_category_3 is null and b.service_category_3 is not null then 'Service cat value not in seed table'
            else s.service_category_3
          end as service_category_3
        , b.service_category_2 as original_service_cat_2
        , b.service_category_3 as original_service_cat_3
        , s.priority
        , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
        , b.source_model_name
    from {{ ref('service_category_grouper__stg_medical_claim') }} as a
    left outer join {{ ref('service_category_grouper__combined_institutional_header_level') }} as b
      on a.claim_id = b.claim_id
      and a.data_source = b.data_source
    left outer join {{ ref('service_category_grouper__service_categories') }} as s
      on b.service_category_1 = s.service_category_1
      and b.service_category_2 = s.service_category_2
      and b.service_category_3 = s.service_category_3
    where a.claim_type = 'institutional'

    union all

    select distinct
        a.claim_id
        , a.claim_line_number
        , a.data_source
        , a.claim_type
        , case when s.service_category_1 is null and b.service_category_1 is not null then 'Service cat value not in seed table'
            else b.service_category_1
          end as service_category_1
        , case when s.service_category_2 is null and b.service_category_2 is not null then 'Service cat value not in seed table'
            else s.service_category_2
          end as service_category_2
        , case
            when s.service_category_3 is null and b.service_category_3 is not null then 'Service cat value not in seed table'
            else s.service_category_3
          end as service_category_3
        , b.service_category_2 as original_service_cat_2
        , b.service_category_3 as original_service_cat_3
        , s.priority
        , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
        , b.source_model_name
    from {{ ref('service_category_grouper__stg_medical_claim') }} as a
    left outer join {{ ref('service_category_grouper__combined_institutional_line_level') }} as b
      on a.claim_id = b.claim_id
      and a.claim_line_number = b.claim_line_number
      and a.data_source = b.data_source
    left outer join {{ ref('service_category_grouper__service_categories') }} as s
      on b.service_category_1 = s.service_category_1
      and b.service_category_2 = s.service_category_2
      and b.service_category_3 = s.service_category_3
    where a.claim_type = 'institutional'
)

, service_category_2_deduplication as (
    select
        claim_id
        , claim_line_number
        , data_source
        , claim_type
        , service_category_1
        , service_category_2
        , service_category_3
        , original_service_cat_2
        , original_service_cat_3
        , source_model_name
        , row_number() over (partition by claim_id, claim_line_number
order by coalesce(priority, 99999)) as duplicate_row_number
    from service_category_1_mapping
)

/* We're bringing in ALL priorities to the final table for use in encounter flags
(For example, regardless of final priority, we might still want to know if a claim could
ever be considered "ED" to flag inpatient events that came in through ED

Filter on this table where duplicate_row_number = 1 to avoid duplicates
*/


select
    d.claim_id
    , d.claim_line_number
    , d.data_source
    , d.claim_type
    , coalesce(d.service_category_1, 'other') as service_category_1
    , coalesce(d.service_category_2, 'other') as service_category_2
    , coalesce(d.service_category_3, 'other') as service_category_3
    , d.original_service_cat_2
    , d.original_service_cat_3
    , d.duplicate_row_number
    , s.ccs_category
    , s.ccs_category_description
    , s.drg_code
    , s.drg_description
    , s.place_of_service_code
    , s.place_of_service_description
    , s.revenue_center_code
    , s.revenue_center_description
    , s.hcpcs_code
    , s.default_ccsr_category_ip
    , s.default_ccsr_category_op
    , s.default_ccsr_category_description_ip
    , s.default_ccsr_category_description_op
    , s.primary_taxonomy_code
    , s.primary_specialty_description
    , s.modality
    , s.bill_type_code
    , s.bill_type_description
    , d.source_model_name
from service_category_2_deduplication as d
inner join {{ ref('service_category_grouper__stg_medical_claim') }} as s
  on d.claim_id = s.claim_id
  and d.claim_line_number = s.claim_line_number
  and d.data_source = s.data_source
