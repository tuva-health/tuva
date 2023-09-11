{{ config(enabled=var('ed_classification_enabled',var('tuva_packages_enabled',True))) }}

select
    *
from {{ ref('ed_classification__int_condition_with_claim') }}
inner join {{ ref('ed_classification__categories') }} using(classification)
