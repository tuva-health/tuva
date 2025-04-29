{{ config(
     enabled = (  var('enable_normalize_engine', False) == True
               ) | as_bool
   )
}}
with agg_cte as (
    select * From {{ref('normalize__stg_unmapped_lab_result')}} union all
    select * From {{ref('normalize__stg_unmapped_medication')}} union all
    select * From {{ref('normalize__stg_unmapped_observation')}} union all
    select * From {{ref('normalize__stg_unmapped_condition')}} union all
    select * From {{ref('normalize__stg_unmapped_procedure')}}
)

select source_code_type,
       source_code,
       source_description,
       item_count,
       domains,
       data_sources,
       normalized_code_type,
       normalized_code,
       normalized_description,
       not_mapped,
       added_by,
       added_date,
       reviewed_by,
       reviewed_date,
       notes
from {{ ref('custom_mapped') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select un.source_code_type,
       un.source_code,
       un.source_description,
       un.item_count,
       un.domains,
       un.data_sources,
       un.normalized_code_type,
       un.normalized_code,
       un.normalized_description,
       un.not_mapped,
       un.added_by,
       un.added_date,
       un.reviewed_by,
       un.reviewed_date,
       un.notes
    from {{ref('normalize__all_unmapped')}} un 
    left join  {{ ref('custom_mapped') }} custom_mapped
    on  ( lower(un.source_code_type) = lower(custom_mapped.source_code_type)
        or ( un.source_code_type is null and custom_mapped.source_code_type is null)
        )
    and (un.source_code = custom_mapped.source_code
        or ( un.source_code is null and custom_mapped.source_code is null)
        )
    and (un.source_description = custom_mapped.source_description
        or ( un.source_description is null and custom_mapped.source_description is null)
        )
    where   custom_mapped.source_code_type is null
        and custom_mapped.source_code is null
        and custom_mapped.source_description is null
