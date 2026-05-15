/* This test checks to ensure that all null gap status are due to one of the following reasons:
- Not a chronic HCC
- Is a suspect HCC (only when it overlaps with an actual claim)
- Was not an eligible claim
- Was filtered out due to a hierarchy

If this test fails, that means that the logic needs to be reviewed to find what other reasons 
are causing ineligible recaptures and, if needed, adjust this test accordingly.
*/
select stat.*
from {{ ref('hcc_recapture__hcc_status') }} as stat
left join {{ ref('hcc_recapture__int_gap_status') }} gaps
    on stat.person_id = gaps.person_id
    and stat.hcc_code = gaps.hcc_code
    and stat.payer = gaps.payer
    and stat.model_version = gaps.model_version
    and stat.payment_year = gaps.payment_year
    and stat.suspect_hcc_flag = gaps.suspect_hcc_flag
left join {{ ref('hcc_recapture__int_recapturable_hccs')}} ext
    on stat.person_id = ext.person_id
    and stat.payer = ext.payer
    and stat.data_source = ext.data_source
    and stat.payment_year = ext.collection_year + 1
    and stat.hcc_code = ext.hcc_code
    and stat.suspect_hcc_flag = ext.suspect_hcc_flag
where stat.gap_status = 'ineligible for recapture'
    and stat.hcc_chronic_flag = 1
    and stat.suspect_hcc_flag = 0
    and stat.eligible_claim_flag = 1
    and stat.filtered_by_hierarchy_flag = 0