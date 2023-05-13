{%
  set medical_cost_cols = dbt_utils.get_column_values(
  table=ref('information_schema'),
  column='column_name',
  where="data_type = 'float' and table_name = 'medical_claim'")
%}
{%
  set pharmacy_cost_cols = dbt_utils.get_column_values(
  table=ref('information_schema'),
  column='column_name',
  where="data_type = 'float' and table_name = 'pharmacy_claim'")
%}
-- depends_on: {{ ref('medical_claim') }}
-- depends_on: {{ ref('pharmacy_claim') }}
-- depends_on: {{ ref('medical_header') }}
-- depends_on: {{ ref('pharmacy_header') }}

{% for column in medical_cost_cols %}
     {{ log("Medical Column: " ~ column, info=true) }}

     select
     'medical_claim' as table_name
     , 'claim_line' as grain
     , '{{ column }}' as column_name
     {{ descriptive_stats(column) }}
     from {{ ref('medical_claim') }}
     group by 1
     union all
{% endfor %}

{% for column in pharmacy_cost_cols %}
     {{ log("Pharmacy Column: " ~ column, info=true) }}

     select
     'pharmacy_claim' as table_name
     , 'claim_line' as grain
     , '{{ column }}' as column_name
     {{ descriptive_stats(column) }}
     from {{ ref('pharmacy_claim') }}
     group by 1
     union all
{% endfor %}

{% for column in medical_cost_cols %}
     {{ log("Medical Column: " ~ column, info=true) }}

     select
     'medical_claim' as table_name
     , 'claim_header' as grain
     , '{{ column }}' as column_name
     {{ descriptive_stats(column) }}
     from {{ ref('medical_header') }}
     group by 1
     union all
{% endfor %}

{% for column in pharmacy_cost_cols %}
     {{ log("Pharmacy Column: " ~ column, info=true) }}

     select
     'pharmacy_claim' as table_name
     , 'claim_header' as grain
     , '{{ column }}' as column_name
     {{ descriptive_stats(column) }}
     from {{ ref('pharmacy_header') }}
     group by 1

     {% if not loop.last %}
     union all
     {% endif %}
{% endfor %}
