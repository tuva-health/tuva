---
id: hcc-recapture
title: "HCC Recapture"
---

## What is HCC Recapture?

The HCC recapture data mart enables organizations to track HCCs which have been coded in a collection year and determine if they have been 'recaptured' (the diagnosis has been coded) in the year after the collection year (i.e. the payment year). This is important because:
- It accurately codes the chronic conditions for a patient 
- It affects that patient's risk score
- The risk score affects reimbursement for value-based care contracts

This mart not only tracks HCCs which were previously coded, but also automatically includes any suspect HCCs from the suspect HCC mart and flags them using the `suspect_hcc_flag`.

Additionally, the mart provides recapture rates and a lot of detail into the type of gap closure.

## Methods

[Code on Github](https://github.com/tuva-health/tuva/tree/main/models/data_marts/hcc_recapture)

The HCC recapture data mart identifies gaps for patients who either an HCC coded or are suspected of an HCC in a collection year.

### Gap Closure
The type of gap closure if provided using the `gap_status` field. Here are the gap status definitions based on the `hcc_recapture__gap_status` model:

| Gap Status | Definition |
|------------|------------|
| closed using higher coefficient hcc in hierarchy group | An HCC in the same group was closed, but its coefficient is greater than the prior year HCC |
| closed | The specific HCC in question has been observed in a risk adjustable claim during the collection year |
| closed using lower coefficient hcc in hierarchy group | An HCC in the same group was closed, but its coefficient is less than the prior year HCC |
| new | Defined as an HCC that has not been coded in the past 2 years |
| open | For gaps and claims, it's a chronic condition appropriate for recapture that has not been documented in current collection year |
| ineligible for recapture | The specific HCC in question is "Open" and no related/equivalent HCC has been closed, but it is not appropriate for risk adjustment because it's not a chronic diagnosis |

Instead of just listing an HCC as closed, more detail is provided which presents an opportunity to improve future HCC recapture initiatives.

### Recapture Curves
When calculating HCC gap closure, YTD recapture curves are often used. Recapture curves are supported within this mart and can be built using the `hcc_recapture__recapture_rates_monthly_ytd` model.

All of the models below are the final models output from the HCC recapture data mart.

