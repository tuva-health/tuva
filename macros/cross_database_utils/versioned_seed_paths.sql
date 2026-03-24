{% macro get_seed_database_folders() %}
  {{ return({
      'concept_library': 'concept-library',
      'reference_data': 'reference-data',
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


{% macro get_seed_bucket(database) %}
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

  {% if bucket is none %}
    {% set bucket = var('custom_bucket_name', 'tuva-public-resources') %}
  {% endif %}

  {% set normalized_bucket = bucket | string | trim %}
  {% if normalized_bucket.startswith('s3://') %}
    {% set normalized_bucket = normalized_bucket[5:] %}
  {% endif %}

  {{ return(normalized_bucket | trim('/')) }}
{% endmacro %}


{% macro get_seed_version(version_override=none) %}
  {% if version_override is none %}
    {% set version = var('tuva_seed_version', '0.18.0') %}
  {% else %}
    {% set version = version_override %}
  {% endif %}

  {% set normalized_version = version | string | trim %}
  {% if normalized_version.startswith('v') %}
    {% set normalized_version = normalized_version[1:] %}
  {% endif %}

  {{ return(normalized_version) }}
{% endmacro %}


{% macro get_versioned_seed_uri(database, version_override=none) %}
  {% set bucket = the_tuva_project.get_seed_bucket(database) %}
  {% set folder = the_tuva_project.get_seed_database_folder(database) %}
  {% set version = the_tuva_project.get_seed_version(version_override) %}
  {{ return(bucket ~ '/' ~ folder ~ '/' ~ version) }}
{% endmacro %}


{% macro load_versioned_seed(database, pattern, version=none, compression=true, headers=true, null_marker=true) %}
  {{ return(the_tuva_project.load_seed(
      the_tuva_project.get_versioned_seed_uri(database, version),
      pattern,
      compression,
      headers,
      null_marker
  )) }}
{% endmacro %}


{% macro get_synthetic_data_size() %}
  {% set synthetic_data_size = var('synthetic_data_size', 'small') | string | trim | lower %}

  {% if synthetic_data_size not in ['small', 'large'] %}
    {% do exceptions.raise_compiler_error(
        "Invalid synthetic_data_size '" ~ synthetic_data_size ~ "'. Expected 'small' or 'large'."
    ) %}
  {% endif %}

  {{ return(synthetic_data_size) }}
{% endmacro %}


{% macro get_synthetic_seed_pattern(seed_name) %}
  {% set synthetic_patterns = {
      'appointment': {
          'small': 'appointment_small.csv',
          'large': 'appointment.csv'
      },
      'eligibility': {
          'small': 'eligibility_small.csv',
          'large': 'eligibility.csv'
      },
      'immunization': {
          'small': 'immunization.csv',
          'large': 'immunization.csv'
      },
      'lab_result': {
          'small': 'lab_result.csv',
          'large': 'lab_result.csv'
      },
      'medical_claim': {
          'small': 'medical_claim_small.csv',
          'large': 'medical_claim.csv'
      },
      'observation': {
          'small': 'observation.csv',
          'large': 'observation.csv'
      },
      'pharmacy_claim': {
          'small': 'pharmacy_claim_small.csv',
          'large': 'pharmacy_claim.csv'
      },
      'provider_attribution': {
          'small': 'provider_attribution.csv',
          'large': 'provider_attribution.csv'
      }
  } %}

  {% if seed_name not in synthetic_patterns %}
    {% do exceptions.raise_compiler_error(
        "Unsupported synthetic seed '" ~ seed_name ~ "'."
    ) %}
  {% endif %}

  {% set synthetic_data_size = the_tuva_project.get_synthetic_data_size() %}
  {{ return(synthetic_patterns[seed_name][synthetic_data_size]) }}
{% endmacro %}


{% macro load_versioned_synthetic_seed(seed_name, version=none, compression=true, headers=true, null_marker=true) %}
  {{ return(the_tuva_project.load_versioned_seed(
      'synthetic_data',
      the_tuva_project.get_synthetic_seed_pattern(seed_name),
      version,
      compression,
      headers,
      null_marker
  )) }}
{% endmacro %}
