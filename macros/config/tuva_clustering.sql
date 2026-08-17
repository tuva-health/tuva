{% macro tuva_cluster_by(columns) %}
{#
    Applies each warehouse's own flavour of "store these rows near each other".

    Claims models join on claim_id and claim_line_number over and over. On a
    large warehouse those joins shuffle unclustered data across every node and
    exhaust shuffle memory; co-locating the join keys is what makes them viable.
    At small volumes it buys nothing, so it is off unless asked for.

    Enable with:  vars: {tuva_clustering_enabled: true}

    Only the BigQuery branch has been exercised. The others are written from
    each adapter's documented config and are unverified -- that is the reason
    this is opt-in rather than on by default. Fabric has no equivalent, so it
    is deliberately absent and falls through to the no-op.

        {{ config(materialized = 'table') }}
        {{ tuva_cluster_by(['claim_id', 'claim_line_number']) }}
#}
{%- if not var('tuva_clustering_enabled', false) -%}
    {{ return('') }}
{%- endif -%}

{%- if target.type in ('bigquery', 'snowflake') -%}
    {%- do config(cluster_by = columns) -%}

{%- elif target.type == 'databricks' -%}
    {%- do config(liquid_clustering = true, clustered_by = columns) -%}

{%- elif target.type == 'redshift' -%}
    {%- do config(sort = columns, sort_type = 'compound') -%}

{%- elif target.type == 'postgres' -%}
    {#- Not clustering: an index is the nearest thing Postgres offers here. -#}
    {%- do config(indexes = [{'columns': columns}]) -%}

{%- elif target.type == 'athena' -%}
    {#- Bucketing needs a bucket count, and the right count depends on the data
        rather than on the model, so it is left to the adapter default. -#}
    {%- do config(bucketed_by = columns, bucket_count = 16) -%}

{%- endif -%}
{% endmacro %}
