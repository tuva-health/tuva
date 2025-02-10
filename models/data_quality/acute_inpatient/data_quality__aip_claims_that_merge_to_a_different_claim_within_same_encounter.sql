{{ config(
    enabled = var('claims_enabled', False)
) }}

with claim_comparisons_within_same_encounter as (

    select
          aa.claim_id as claim_id_a
        , bb.claim_id as claim_id_b
        , aa.merge_start_date as merge_start_date_a
        , aa.merge_end_date as merge_end_date_a
        , aa.discharge_disposition_code as ddc_a
        , aa.facility_npi as facility_npi_a
        , bb.merge_start_date as merge_start_date_b
        , bb.merge_end_date as merge_end_date_b
        , bb.discharge_disposition_code as ddc_b
        , bb.facility_npi as facility_npi_b

        -- Add merge_flag:
        , case
            -- Claims that overlap and have the same facility_npi are merged
            -- into the same encounter:
            when
            (
                (
                    (aa.merge_start_date between bb.merge_start_date and bb.merge_end_date)
                    or
                    (aa.merge_end_date between bb.merge_start_date and bb.merge_end_date)
                    or
                    (bb.merge_start_date between aa.merge_start_date and aa.merge_end_date)
                    or
                    (bb.merge_end_date between aa.merge_start_date and aa.merge_end_date)
                )
                and
                (aa.facility_npi = bb.facility_npi)
            ) then 1

            -- Claims that are adjacent (merge_start_date of the second one is the day
            -- after merge_end_date of the first one) should be merged if the
            -- first claim has discharge_disposition_code = '30' (still a patient)
            -- and they have the same facility_npi:
            when
            (
                ({{ dbt.dateadd (
                        datepart = "day"
                        , interval = 1
                        , from_date_or_timestamp = "aa.merge_end_date" 
                        ) }} = bb.merge_start_date)
                and
                (aa.discharge_disposition_code = '30')
                and
                (aa.facility_npi = bb.facility_npi)
            ) then 1

            else 0
        end as merge_flag

    from {{ ref('data_quality__aip_multiple_claim_encounters') }} aa
    inner join {{ ref('data_quality__aip_multiple_claim_encounters') }} bb
        on aa.person_id = bb.person_id
        and aa.encounter_id = bb.encounter_id
        and aa.claim_id <> bb.claim_id

)

, claim_merges_within_same_encounter as (

    select
          claim_id_a
        , claim_id_b
        , merge_start_date_a
        , merge_end_date_a
        , merge_start_date_b
        , merge_end_date_b
        , ddc_a
        , ddc_b
        , facility_npi_a
        , facility_npi_b
        , merge_flag

    from claim_comparisons_within_same_encounter
    where merge_flag = 1

)

select
      claim_id_a
    , claim_id_b
    , merge_start_date_a
    , merge_end_date_a
    , merge_start_date_b
    , merge_end_date_b
    , ddc_a
    , ddc_b
    , facility_npi_a
    , facility_npi_b
    , merge_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from claim_merges_within_same_encounter
