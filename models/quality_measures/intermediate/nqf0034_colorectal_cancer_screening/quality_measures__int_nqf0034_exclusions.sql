{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with exclusions as (
select *
From {{ref('quality_measures__int_nqf0034_exclude_advanced_illness')}}

union all

select *
From {{ref('quality_measures__int_nqf0034_exclude_colectomy_cancer')}}

union all

select *
From {{ref('quality_measures__int_nqf0034_exclude_dementia')}}

union all

select *
From {{ref('quality_measures__int_nqf0034_exclude_hospice_palliative')}}

union all

select *
From {{ref('quality_measures__int_nqf0034_exclude_institutional_snp')}}
)

select exclusions.*
from exclusions
inner join {{ref('quality_measures__int_nqf0034_denominator')}} p
    on exclusions.patient_id = p.patient_id