{{ config(enabled=var('ed_classification_enabled',var('tuva_packages_enabled',True))) }}

select
    *
from {{ ref('ed_classified_condition_with_claim') }}
inner join {{ ref('ed_classification_categories') }} using(classification)
