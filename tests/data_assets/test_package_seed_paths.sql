{% set synthetic_size = the_tuva_project.get_synthetic_data_size() %}
{% set package_version = the_tuva_project.get_tuva_package_version() %}
{# Resolve expected buckets independently from the macros exercised below. #}
{% set configured_buckets = var('tuva_seed_buckets', {}) %}
{% if configured_buckets is not mapping %}
  {% set configured_buckets = {} %}
{% endif %}
{% set default_bucket = var('custom_bucket_name', 'tuva-public-resources') %}

{% set terminology_bucket = configured_buckets.get('terminology') %}
{% if terminology_bucket is none %}
  {% set terminology_bucket = configured_buckets.get('tuva-core') %}
{% endif %}
{% if terminology_bucket is none %}
  {% set terminology_bucket = configured_buckets.get('tuva_core') %}
{% endif %}
{% if terminology_bucket is none %}
  {% set terminology_bucket = default_bucket %}
{% endif %}

{% set provider_data_bucket = configured_buckets.get('provider_data') %}
{% if provider_data_bucket is none %}
  {% set provider_data_bucket = configured_buckets.get('tuva-core') %}
{% endif %}
{% if provider_data_bucket is none %}
  {% set provider_data_bucket = configured_buckets.get('tuva_core') %}
{% endif %}
{% if provider_data_bucket is none %}
  {% set provider_data_bucket = default_bucket %}
{% endif %}

{% set value_sets_bucket = configured_buckets.get('value_sets') %}
{% if value_sets_bucket is none %}
  {% set value_sets_bucket = configured_buckets.get('tuva-core') %}
{% endif %}
{% if value_sets_bucket is none %}
  {% set value_sets_bucket = configured_buckets.get('tuva_core') %}
{% endif %}
{% if value_sets_bucket is none %}
  {% set value_sets_bucket = default_bucket %}
{% endif %}

{% set package_bucket = configured_buckets.get('cms-hcc') %}
{% if package_bucket is none %}
  {% set package_bucket = configured_buckets.get('cms_hcc') %}
{% endif %}
{% if package_bucket is none %}
  {% set package_bucket = default_bucket %}
{% endif %}

{% set terminology_bucket = terminology_bucket | string | trim %}
{% if terminology_bucket.startswith('s3://') %}
  {% set terminology_bucket = terminology_bucket[5:] %}
{% endif %}
{% set terminology_bucket = terminology_bucket | trim('/') %}

{% set provider_data_bucket = provider_data_bucket | string | trim %}
{% if provider_data_bucket.startswith('s3://') %}
  {% set provider_data_bucket = provider_data_bucket[5:] %}
{% endif %}
{% set provider_data_bucket = provider_data_bucket | trim('/') %}

{% set value_sets_bucket = value_sets_bucket | string | trim %}
{% if value_sets_bucket.startswith('s3://') %}
  {% set value_sets_bucket = value_sets_bucket[5:] %}
{% endif %}
{% set value_sets_bucket = value_sets_bucket | trim('/') %}

{% set package_bucket = package_bucket | string | trim %}
{% if package_bucket.startswith('s3://') %}
  {% set package_bucket = package_bucket[5:] %}
{% endif %}
{% set package_bucket = package_bucket | trim('/') %}

with resolved_paths as (
  select
    '{{ the_tuva_project.get_versioned_seed_uri("terminology") }}' as terminology_uri
    , '{{ the_tuva_project.get_versioned_seed_uri("provider_data") }}' as provider_data_uri
    , '{{ the_tuva_project.get_versioned_seed_uri("value_sets") }}' as value_sets_uri
    , '{{ the_tuva_project.get_synthetic_seed_object_path("synthetic_data__appointment") }}' as synthetic_object_path
    , '{{ the_tuva_project.get_package_seed_uri("cms-hcc", "v1.0.0", "cms_hcc__sample.csv.gz", "s3://custom-assets/") }}' as generic_package_uri
    , '{{ the_tuva_project.get_package_seed_uri("cms-hcc", "1.0.0", "cms_hcc__sample.csv.gz") }}' as overridden_package_uri
)

select *
from resolved_paths
where terminology_uri <> '{{ terminology_bucket }}/tuva-core/{{ package_version }}/terminology'
  or provider_data_uri <> '{{ provider_data_bucket }}/tuva-core/{{ package_version }}/provider-data'
  or value_sets_uri <> '{{ value_sets_bucket }}/tuva-core/{{ package_version }}/value-sets'
  or synthetic_object_path <> 'synthetic-data/{{ synthetic_size }}/synthetic_data__appointment.csv.gz'
  or generic_package_uri <> 'custom-assets/cms-hcc/1.0.0/cms_hcc__sample.csv.gz'
  or overridden_package_uri <> '{{ package_bucket }}/cms-hcc/1.0.0/cms_hcc__sample.csv.gz'
