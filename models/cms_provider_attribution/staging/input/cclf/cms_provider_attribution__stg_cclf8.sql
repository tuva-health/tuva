
with base as (
select 
  {% for column in adapter.get_columns_in_relation(source('phds_lakehouse_test', 'yak_cclf_8')) %}
    {% if column.name != 'ingest_datetime' %}
      cast(nullif(trim({{ column.name }}), '') as {{ dbt.type_string() }}) as {{ column.name }}
    {% else %}
      {{ column.name }}
    {% endif %}
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}
from {{ source('phds_lakehouse_test', 'yak_cclf_8') }}
)

, cclf8 as (
select 
      CONCAT('A',
      SUBSTRING(filename, CHARINDEX('P.A', filename) + 3, 4
                )) as aco_id
    , cast(bene_mbi_id as {{ dbt.type_string() }}) as person_id
    , cast(bene_fips_state_cd as {{ dbt.type_string() }}) as bene_fips_state_cd
    , cast(bene_death_dt as {{ dbt.type_string() }}) as bene_death_dt
    , cast(bene_rng_bgn_dt as {{ dbt.type_string() }}) as bene_rng_bgn_dt
    , cast(bene_rng_end_dt as {{ dbt.type_string() }}) as bene_rng_end_dt
    , cast(bene_entlmt_buyin_ind as {{ dbt.type_string() }}) as bene_entlmt_buyin_ind
    , cast(filename as {{ dbt.type_string() }}) as file_name
    , cast(
            TRY_CONVERT(date, 
                SUBSTRING(filename, 
                    CHARINDEX('.D', filename) + 2, 6
                ), 
                12
            ) 
        AS date) AS file_date
    , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime

from base
)

, extract_performance_year as (
select 
  cclf8.*
  , SUBSTRING(file_name, 
      CHARINDEX('.D', file_name) - 3, 3
    ) as performance_year_base
from cclf8
)

, add_performance_year as (
select
    cclf8.* 
  , 2000 + substring(performance_year_base,2,2) as performance_year 
  , case when upper(performance_year_base) like 'R%' then 1 else 0 end as runout_file
from extract_performance_year cclf8
)

select distinct
    aco_id
  , person_id
  , performance_year
  , datefromparts(year(base.file_date), month(base.file_date), 1) as coverage_month
  , bene_entlmt_buyin_ind
  , bene_fips_state_cd
  , bene_death_dt
  , runout_file
from add_performance_year base
