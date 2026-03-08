-- This test verifies that any institutional claim flagged as having duplicate
-- diagnosis codes actually has at least one code repeated across positions.
{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool,
     tags = ['dqi', 'tuva_dqi_sev_4'],
     severity = 'warn'
   )
}}

with flagged_duplicates as (
    select
          drill_down_value as claim_id
        , data_source
    from {{ ref('data_quality__institutional_diagnosis_code_uniqueness') }}
    where bucket_name = 'duplicate'
)

, medical_claims as (
    select
          claim_id
        , data_source
        , diagnosis_code_1
        , diagnosis_code_2
        , diagnosis_code_3
        , diagnosis_code_4
        , diagnosis_code_5
        , diagnosis_code_6
        , diagnosis_code_7
        , diagnosis_code_8
        , diagnosis_code_9
        , diagnosis_code_10
        , diagnosis_code_11
        , diagnosis_code_12
        , diagnosis_code_13
        , diagnosis_code_14
        , diagnosis_code_15
        , diagnosis_code_16
        , diagnosis_code_17
        , diagnosis_code_18
        , diagnosis_code_19
        , diagnosis_code_20
        , diagnosis_code_21
        , diagnosis_code_22
        , diagnosis_code_23
        , diagnosis_code_24
        , diagnosis_code_25
    from {{ ref('medical_claim') }}
    where claim_type = 'institutional'
)

, flagged_claims as (
    select
          mc.claim_id
        , mc.data_source
        {% for i in range(1, 26) %}
        , mc.diagnosis_code_{{ i }}
        {% endfor %}
    from medical_claims as mc
    inner join flagged_duplicates as fd
        on mc.claim_id = fd.claim_id
        and mc.data_source = fd.data_source
)

, unpivot_dx as (
    {% for i in range(1, 26) %}
    select
          claim_id
        , data_source
        , diagnosis_code_{{ i }} as diagnosis_code
    from flagged_claims
    where diagnosis_code_{{ i }} is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

, dx_counts as (
    select
          claim_id
        , data_source
        , diagnosis_code
        , count(*) as occurrences
    from unpivot_dx
    group by
          claim_id
        , data_source
        , diagnosis_code
    having count(*) > 1
)

-- Returns claims flagged as duplicates that have NO actual duplicate codes.
-- A passing test returns zero rows.
select
      fd.claim_id
    , fd.data_source
from flagged_duplicates as fd
left outer join dx_counts as dc
    on fd.claim_id = dc.claim_id
    and fd.data_source = dc.data_source
where dc.claim_id is null
