/* Deceased Beneficiaries: Section 2.3.2.2 Prospective Assignment

"Also note that in determining prospective assignment for ACOs’ benchmark and performance
years, CMS identifies beneficiaries who, although assigned using the offset assignment window
(October–September), died prior to the start of the benchmark or performance year. CMS
excludes these deceased beneficiaries from use in quarterly reports, determining financial
reconciliation for the performance year and in determining benchmark year assignment. "

This is interpreted as applying to both prospective AND retrospective assignment. Likely is not mentioned
for retrospective assignment since the preliminary list is provided after the prospective assignment window
and it's assumed there are no dead benes within the initial AALR file.
*/

select distinct
      aco_id
    , person_id
from {{ref('cms_provider_attribution__stg_beneficiary_demographics')}} 
where death_date < DATEFROMPARTS(performance_year,1,1)