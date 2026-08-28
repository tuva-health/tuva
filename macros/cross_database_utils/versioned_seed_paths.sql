{% macro get_seed_database_folders() %}
  {{ return({
      'terminology': 'terminology',
      'value_sets': 'value-sets',
      'provider_data': 'provider-data',
      'synthetic_data': 'synthetic-data'
  }) }}
{% endmacro %}


{% macro get_seed_database_folder(database) %}
  {% set folders = the_tuva_project.get_seed_database_folders() %}
  {% set normalized_database = database | string | trim %}
  {% set alternate_database = normalized_database | replace('-', '_') %}

  {% if normalized_database in folders %}
    {{ return(folders[normalized_database]) }}
  {% endif %}

  {% if alternate_database in folders %}
    {{ return(folders[alternate_database]) }}
  {% endif %}

  {% for folder in folders.values() %}
    {% if normalized_database == folder %}
      {{ return(folder) }}
    {% endif %}
  {% endfor %}

  {% do exceptions.raise_compiler_error(
      "Unsupported Tuva seed database '" ~ database ~ "'."
  ) %}
{% endmacro %}


{% macro get_seed_bucket(database, package_slug=none) %}
  {% set bucket_overrides = var('tuva_seed_buckets', {}) %}
  {% set normalized_database = database | string | trim %}
  {% set alternate_database = normalized_database | replace('-', '_') %}
  {% set bucket = none %}

  {% if bucket_overrides is mapping %}
    {% if normalized_database in bucket_overrides %}
      {% set bucket = bucket_overrides[normalized_database] %}
    {% elif alternate_database in bucket_overrides %}
      {% set bucket = bucket_overrides[alternate_database] %}
    {% endif %}
  {% endif %}

  {% if bucket is none and package_slug is not none and bucket_overrides is mapping %}
    {% set normalized_slug = package_slug | string | trim %}
    {% set alternate_slug = normalized_slug | replace('-', '_') %}
    {% if normalized_slug in bucket_overrides %}
      {% set bucket = bucket_overrides[normalized_slug] %}
    {% elif alternate_slug in bucket_overrides %}
      {% set bucket = bucket_overrides[alternate_slug] %}
    {% endif %}
  {% endif %}

  {% if bucket is none %}
    {% set bucket = var('custom_bucket_name', 'tuva-public-resources') %}
  {% endif %}

  {% set normalized_bucket = bucket | string | trim %}
  {% if normalized_bucket.startswith('s3://') %}
    {% set normalized_bucket = normalized_bucket[5:] %}
  {% endif %}

  {{ return(normalized_bucket | trim('/')) }}
{% endmacro %}


{% macro get_package_seed_uri(package_slug, package_version, object_path='', bucket=none) %}
  {% set normalized_slug = package_slug | string | trim | trim('/') %}
  {% if normalized_slug == '' or '/' in normalized_slug %}
    {% do exceptions.raise_compiler_error(
        "Tuva package seed slug must be a nonempty path segment. Received '" ~ package_slug ~ "'."
    ) %}
  {% endif %}

  {% set normalized_version = package_version | string | trim %}
  {% if normalized_version.startswith('v') %}
    {% set normalized_version = normalized_version[1:] %}
  {% endif %}
  {% set normalized_version = normalized_version | trim('/') %}
  {% if normalized_version == '' or '/' in normalized_version %}
    {% do exceptions.raise_compiler_error(
        "Tuva package seed version must be a nonempty path segment. Received '" ~ package_version ~ "'."
    ) %}
  {% endif %}

  {% if bucket is none %}
    {% set normalized_bucket = the_tuva_project.get_seed_bucket(normalized_slug) %}
  {% else %}
    {% set normalized_bucket = bucket | string | trim %}
    {% if normalized_bucket.startswith('s3://') %}
      {% set normalized_bucket = normalized_bucket[5:] %}
    {% endif %}
    {% set normalized_bucket = normalized_bucket | trim('/') %}
  {% endif %}

  {% set normalized_object_path = object_path | string | trim | trim('/') %}
  {% set uri = normalized_bucket ~ '/' ~ normalized_slug ~ '/' ~ normalized_version %}
  {% if normalized_object_path != '' %}
    {% set uri = uri ~ '/' ~ normalized_object_path %}
  {% endif %}

  {{ return(uri) }}
{% endmacro %}


{% macro load_package_seed(package_slug, package_version, object_path, compression=true, headers=true, null_marker=true, bucket=none) %}
  {% set normalized_object_path = object_path | string | trim | trim('/') %}
  {% if normalized_object_path == '' %}
    {% do exceptions.raise_compiler_error("Tuva package seed object_path must not be empty.") %}
  {% endif %}

  {% set object_parts = normalized_object_path.split('/') %}
  {% set pattern = object_parts[-1] %}
  {% set object_folder = object_parts[:-1] | join('/') %}

  {{ return(the_tuva_project.load_seed(
      the_tuva_project.get_package_seed_uri(
          package_slug,
          package_version,
          object_folder,
          bucket
      ),
      pattern,
      compression,
      headers,
      null_marker
  )) }}
{% endmacro %}


{% macro get_versioned_seed_uri(database, version_override=none) %}
  {% if version_override is not none %}
    {% do exceptions.raise_compiler_error(
        "Per-family Tuva seed version overrides were removed in 1.0. "
        ~ "Data assets must use the installed Tuva Core package version."
    ) %}
  {% endif %}

  {{ return(the_tuva_project.get_package_seed_uri(
      'tuva-core',
      the_tuva_project.get_tuva_package_version(),
      the_tuva_project.get_seed_database_folder(database),
      the_tuva_project.get_seed_bucket(database, 'tuva-core')
  )) }}
{% endmacro %}


{% macro load_versioned_seed(database, seed_object_name, version=none, compression=true, headers=true, null_marker=true) %}
  {% if version is not none %}
    {% do exceptions.raise_compiler_error(
        "Per-family Tuva seed version overrides were removed in 1.0. "
        ~ "Data assets must use the installed Tuva Core package version."
    ) %}
  {% endif %}

  {% set object_path = the_tuva_project.get_seed_database_folder(database) ~ '/' ~ seed_object_name %}
  {{ return(the_tuva_project.load_package_seed(
      'tuva-core',
      the_tuva_project.get_tuva_package_version(),
      object_path,
      compression,
      headers,
      null_marker,
      the_tuva_project.get_seed_bucket(database, 'tuva-core')
  )) }}
{% endmacro %}


{% macro get_synthetic_data_size() %}
  {% set synthetic_data_size = var('synthetic_data_size', 'large') | string | trim | lower %}

  {% if synthetic_data_size not in ['small', 'large'] %}
    {% do exceptions.raise_compiler_error(
        "Invalid synthetic_data_size '" ~ synthetic_data_size ~ "'. Expected 'small' or 'large'."
    ) %}
  {% endif %}

  {{ return(synthetic_data_size) }}
{% endmacro %}


{% macro get_synthetic_seed_object_path(seed_name) %}
  {% set synthetic_seeds = [
      'synthetic_data__appointment',
      'synthetic_data__condition',
      'synthetic_data__eligibility',
      'synthetic_data__encounter',
      'synthetic_data__immunization',
      'synthetic_data__lab_result',
      'synthetic_data__location',
      'synthetic_data__medical_claim',
      'synthetic_data__medication',
      'synthetic_data__observation',
      'synthetic_data__patient',
      'synthetic_data__pharmacy_claim',
      'synthetic_data__practitioner',
      'synthetic_data__procedure',
      'synthetic_data__provider_attribution'
  ] %}

  {% if seed_name not in synthetic_seeds %}
    {% do exceptions.raise_compiler_error(
        "Unsupported synthetic seed '" ~ seed_name ~ "'."
    ) %}
  {% endif %}

  {% set synthetic_data_size = the_tuva_project.get_synthetic_data_size() %}
  {{ return('synthetic-data/' ~ synthetic_data_size ~ '/' ~ seed_name ~ '.csv.gz') }}
{% endmacro %}


{% macro load_versioned_synthetic_seed(seed_name, version=none, compression=true, headers=true, null_marker=true) %}
  {% if version is not none %}
    {% do exceptions.raise_compiler_error(
        "Synthetic-data version overrides were removed in 1.0. "
        ~ "Data assets must use the installed Tuva Core package version."
    ) %}
  {% endif %}

  {{ return(the_tuva_project.load_package_seed(
      'tuva-core',
      the_tuva_project.get_tuva_package_version(),
      the_tuva_project.get_synthetic_seed_object_path(seed_name),
      compression,
      headers,
      null_marker,
      the_tuva_project.get_seed_bucket('synthetic_data', 'tuva-core')
  )) }}
{% endmacro %}
