{% set synthetic_size = the_tuva_project.get_synthetic_data_size() %}
{% set package_version = the_tuva_project.get_tuva_package_version() %}
{% set expected_terminology_bucket = var('expected_terminology_bucket', 'tuva-public-resources') %}
{% set expected_core_bucket = var('expected_core_bucket', 'tuva-public-resources') %}
{% set expected_package_bucket = var('expected_package_bucket', 'tuva-public-resources') %}

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
where terminology_uri <> '{{ expected_terminology_bucket }}/tuva-core/{{ package_version }}/terminology'
  or provider_data_uri <> '{{ expected_core_bucket }}/tuva-core/{{ package_version }}/provider-data'
  or value_sets_uri <> '{{ expected_core_bucket }}/tuva-core/{{ package_version }}/value-sets'
  or synthetic_object_path <> 'synthetic-data/{{ synthetic_size }}/synthetic_data__appointment.csv.gz'
  or generic_package_uri <> 'custom-assets/cms-hcc/1.0.0/cms_hcc__sample.csv.gz'
  or overridden_package_uri <> '{{ expected_package_bucket }}/cms-hcc/1.0.0/cms_hcc__sample.csv.gz'
