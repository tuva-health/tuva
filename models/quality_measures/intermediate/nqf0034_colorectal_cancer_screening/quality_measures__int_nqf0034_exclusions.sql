{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

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
