---
id: attribution
title: "Attribution"
---

Provider attribution is the means of attributing individual patients to a provider, often times a primary care 
provider, using medical claims or electronic medical record data.  There are many reasons why one would like attribute patients to providers, from evaluating performance to understanding healthcare outcomes.

There is not "one way" to approach provider attribution, and there are many valid approaches to how to go about 
attributing patients to providers. In this section, we discuss provider attribution at a high-level and look 
at one of the attribution methods used by the Center of Medicare and Medicaid services (CMS) in the context of their 
Next Gen ACO models, Direct Contracting Entity models, and ACO REACH programs (the same attribution model being used in all these programs).

Before getting started, here are some good questions to consider related to provider attribution:

- How do you determine if a provider is a primary care provider?
- Should a blood draw or other lab test count in the attribution process?
- If a patient doesn't visit a provider in a long time, should they be attributed to a provider?  How long is too long?
- How do you handle if a patient is seeing multiple providers in a short period of time?
- What data fields are available to you from medical claims data that should be used in provider attribution?

## CMS Attribution Model

This example will use a single attribution model from some of CMS's CMMI programs. Full details about this attribution model can be found [here](https://www.cms.gov/priorities/innovation/media/document/dc-financial-op-guide-overview) in "Appendix B".  This attribution model from CMS uses exclusively Medicare claims data to attribute Medicare patients to providers. 

Specific data used from the claims data are:

1.	**Rendering Provider NPI:** Used to determine if provider is a primary care provider by looking at the provider's specialty
2.	**HCPCS Codes:** Used to determine if service is a primary care service (looking for E&M codes)
3.	**Place of Service Codes:** Used to determine if service is primary care service (looking for office visit care setting)
4.	**Date of Service:** Used for timing
5.	**Claim Allowed Amounts:** Used for weighting

Other sources of data are used as well for attribution in this model:

1.	**[NPPES](https://npiregistry.cms.hhs.gov/search):** Used to determine if a provider is a primary care provider via the provider’s primary care taxonomy
2.	**[Taxonomy Crosswalk](https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/medicare-provider-and-supplier-taxonomy-crosswalk) Data:** Crosswalks Medicare specialty code to provider taxonomy code

The NPPES and Taxonomy data is cleaned up and organized in the Tuva Project in the terminology.provider table.

The attribution model CMS uses to attribute patients to providers is used in the context of primary care. First CMS only looks at a subset of providers it deems as "primary care." Then for a given patient, the attribution model looks at medical claims data for this subset of primary care providers and looks at which provider that patient has seen the most, with some weighting applied to give preference to more recently seen providers. Let’s walk through
an example patient.

![Example Patient Overview](/img/provider_attribution/provider_attribution_overview.drawio.svg)

Below we describe the steps in more detail.

1. **Pull all medical claims data for a patient (professional claims only).**

2. **Filter the medical claims data based on the rendering provider NPI and crosswalks to filter to just primary 
care providers.** Based on the rendering provider NPI on the claim, lookup that provider's taxonomy on the [NPI
Registry Database](https://npiregistry.cms.hhs.gov/search).

![primary_care_specialty_codes](/img/provider_attribution/primary_care_specialty_codes.png)

Here are the primary care specialty codes specified in this attribution model.

![primary_care_specialty_code_taxonomy_crosswalk](/img/provider_attribution/primary_care_specialty_code_taxonomy_crosswalk.png)

They need to be taken in context of the crosswalk to get from specialty code to taxonomy code. 

3. **Filter out any claims that are not in the PQEM procedure code set provided.**

![primary_care_procedure_codes](/img/provider_attribution/primary_care_procedure_codes.png)

Here's a subset of some of the procedure codes that are considered primary care services.  The full list can be found on page 40 of [this](https://www.cms.gov/priorities/innovation/media/document/dc-financial-op-guide-overview)
pdf. Some have a place of service code pre-requisite to be considered valid.

4. **Filter out claims that do not meet the date range criteria for the alignment period.**

5. **Apply weighting to allowed amounts and sum by provider. Claims in the earlier alignment period should be weighted by 1/3, and claims in the more recent alignment period should be weighted 2/3.**

![example_alignment_periods](/img/provider_attribution/alignment_year_date_ranges.png)

6. **The provider with the most weighted dollars gets the patient attributed.  In the case of a tie, choose the provider with the more recent claim.**

**Possible Issues with this Model**

* This model will not attribute patients who have not seen their primary care provider in the two-year observation 
period.
* This model will end up not weighting care provided by Advanced Practice Providers (APPs) less than care provided 
by MDs and DOs.
* This model might have specialty care services competing with primary care services due to the taxonomy classification
system and crosswalk not reflecting APPs providing specialist care.
* This model might not be a timely reflection for attributing patients to providers. It may take up to two years for 
a patient to get appropriately attributed to a provider after a change in provider. 

**Additional Considerations**

* How are you going to use the model? Is your use care specific to primary care?
* Does it make sense to use allowed amounts?
* How should timing of visits be taken into account?
* Is there other sources besides medical claims data that you have available to use?
* How should APP visits be handled?
* Does your model need to be straight forward and explainable to others?

## References 
This page just took a look at a single attribution model. More attribution models exist.
* https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6549236/
