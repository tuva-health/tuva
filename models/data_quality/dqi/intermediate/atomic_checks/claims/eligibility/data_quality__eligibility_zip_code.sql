{{ config(
    enabled = var('claims_enabled', False)
) }}

select distinct -- to bring to claim_ID grain
    m.data_source
    ,coalesce(cast(m.enrollment_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    ,'ELIGIBILITY' as table_name
    ,'Member ID | Enrollment Start Date' as drill_down_key
    ,coalesce(m.member_id, 'NULL') as drill_down_value
    ,'ELIGIBILITY' as claim_type
    ,'ZIP_CODE' as field_name
    {% if target.type == 'fabric' %}
    ,case when m.zip_code is  null then 'null'
          when len(m.zip_code) in  (5,9,10) then 'valid'
                             else 'invalid' end as bucket_name
    ,case
        when m.zip_code is not null and len(m.zip_code) NOT IN (5,9,10) then 'Invalid Zip Code Length'
        else null
     end as invalid_reason
    {% else %}
    ,case when m.zip_code is  null then 'null'
          when length(m.zip_code) in  (5,9,10) then 'valid'
                             else 'invalid' end as bucket_name
    ,case
        when m.zip_code is not null and length(m.zip_code) not in (5,9,10) then 'Invalid Zip Code Length'
        else null
     end as invalid_reason
    {% endif %}
    ,cast(zip_code as {{ dbt.type_string() }}) as field_value
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('eligibility') }} as m
