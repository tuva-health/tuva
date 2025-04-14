
{% if var('enable_normalize_engine',false) == True %}

select i.SOURCE_CODE_TYPE, i.SOURCE_CODE, i.SOURCE_DESCRIPTION,
       count(*) as item_count,
       'observation' as domain,
       i.data_source
from {{ref('core__observation')}} i
left join {{ ref('custom_mapped') }} custom_mapped
    on  ( lower(i.source_code_type) = lower(custom_mapped.source_code_type)
        or ( i.source_code_type is null and custom_mapped.source_code_type is null)
        )
    and (i.source_code = custom_mapped.source_code
        or ( i.source_code is null and custom_mapped.source_code is null)
        )
    and (i.source_description = custom_mapped.source_description
        or ( i.source_description is null and custom_mapped.source_description is null)
        )
where i.NORMALIZED_CODE is null and i.NORMALIZED_DESCRIPTION is null
    and not ( i.SOURCE_CODE is null and i.SOURCE_DESCRIPTION is null)
    and custom_mapped.not_mapped is null
group by i.SOURCE_CODE_TYPE, i.SOURCE_CODE, i.SOURCE_DESCRIPTION, i.data_source

{% else %}

select i.SOURCE_CODE_TYPE, i.SOURCE_CODE, i.SOURCE_DESCRIPTION,
       count(*) as item_count,
       'observation' as domain,
       i.data_source
from {{ref('core__observation')}} i
where i.NORMALIZED_CODE is null and i.NORMALIZED_DESCRIPTION is null
    and not ( i.SOURCE_CODE is null and i.SOURCE_DESCRIPTION is null)
group by i.SOURCE_CODE_TYPE, i.SOURCE_CODE, i.SOURCE_DESCRIPTION, i.data_source

{% endif %}