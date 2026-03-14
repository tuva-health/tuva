---
id: risk-adjusted-benchmarking
title: "Risk-adjusted Benchmarking"
---


## Why Benchmark

Healthcare organizations need reliable ways to compare their performance against expected outcomes to identify quality and cost improvement opportunities, track progress against goals, and demonstrate impact to stakeholders. Benchmarking helps answer critical strategic questions, such as:

**Performance Assessment** : How is our organization performing compared to what would be expected given our patient population characteristics?  
**Resource Allocation**: Where should we focus improvement efforts to maximize impact?  
**Contract Negotiation**: What targets are realistic for our specific patient mix in risk-based contracts?  
**Fairness in Comparison**: How can we ensure we're making "apples-to-apples" comparisons when evaluating performance across different practices, regions, or time periods?

## The Model-Based Approach to Benchmarking

One common approach to benchmarking is to use population-level statistics derived from a reference dataset, such as national or regional claims data. Adjustment factors are then applied for a limited set of demographic variables—typically age, sex, geography, and a small number of clinical groupings. These factors are used to compute expected values for cost or utilization by subgroup, which are then rolled up into population-level benchmarks.

This method is widely used in regulatory and actuarial contexts due to its transparency and simplicity. However, it has several limitations:

- **Data representativeness:** High-quality benchmarks require large, representative datasets, which can be difficult to access.
- **Limited flexibility:** Adjustments are usually confined to a few pre-defined variables.
- **Resource-intensive:** Manually deriving and maintaining adjustment factors can be laborious and computationally expensive.
- **Lower precision:** Group-level adjustments often fail to capture complex, patient-specific variation in risk.
Predictive Modeling with Machine Learning

A more targeted and scalable alternative is to use predictive models that estimate expected values based on a richer set of features, including demographics, chronic conditions, prior utilization, and social factors. These models are typically trained on historical data and generate patient-level predictions using techniques such as regularized regression, gradient-boosted trees (e.g., XGBoost), or other machine learning methods.

This approach has several advantages:

- **Granularity:** Models generate individualized predictions rather than relying on group averages.
- **Scalability:** New features and updated data can be incorporated quickly.
- **Higher accuracy:** Machine learning models can capture nonlinear relationships and interactions that traditional methods miss.
- **Temporal flexibility:** Predictions can be generated at the member-year or member-month level, enabling trend analysis over time.

Importantly, modern predictive models must be carefully calibrated and validated to ensure they produce reliable and fair results across different patient subgroups. Tools like SHAP values can also be used to enhance model transparency.

## Adjusting Benchmarks for the Right Factors: What to Include (and Exclude) in Benchmarking Models

---

When building predictive models for benchmarking, the goal is not simply to maximize prediction accuracy. Instead, it’s to fairly compare outcomes across healthcare organizations by adjusting for differences in patient risk.

To do this effectively, your model should include only factors that are outside the control of the health system. This ensures that observed differences in cost, utilization, or outcomes reflect true differences in performance and not differences in patient mix.

#### Include variables that reflect *inherent patient risk*:
- **Demographics**: Age, sex, geography
- **Chronic conditions**: Comorbidity indices, diagnosis groups
- **Other exogenous factors**: If available, area-level social determinants (e.g., SVI)

These are factors providers cannot change, and they help level the playing field when comparing across systems.

#### Exclude variables that reflect *healthcare system influence*:
- Number of emergency department (ED) visits
- Prior year’s total cost or utilization
- Whether the patient has an assigned PCP
- History of wellness visits or screenings

These are outcomes or engagement indicators that health systems can (and should) influence. Including them in a risk adjustment model would essentially "credit" or "penalize" systems for their own performance—undermining the point of benchmarking.

---

#### Why this matters:

Imagine a health system with strong primary care and care management. They've successfully reduced ED visits by 30%.  
If your benchmarking model includes prior ED visits as a predictor, the system’s improved performance will be baked into the expected values—making them appear average, even though they’re outperforming peers.

By excluding provider-driven variables, you ensure that:
- Performance differences remain visible
- Benchmarks are fair and actionable
- The model encourages—not punishes—improvement

---

In short, benchmarking models are not about explaining everything, but rather adjusting only for what’s outside the system’s control so that what remains can meaningfully reflect performance.


## Prospective vs. Concurrent Models

---

Predictive models in healthcare typically fall into two categories based on their time orientation: **prospective** and **concurrent** models. Both use patient-level data and similar modeling techniques, but they differ in what they aim to predict and how their outputs are used.


### **Prospective Models**

**Purpose:** Predict future outcomes based on historical data.

These models use past or current-year patient data to estimate what is likely to happen in a future period. They are commonly used for forward-looking activities, such as care management targeting, financial forecasting, and value-based contract planning.

- **Use case:** Identifying high-risk patients for intervention before costly events occur
- **Example:** Use 2024 patient demographics and clinical history to predict 2025 medical expenditures


### **Concurrent Models**

**Purpose:** Estimate expected performance for the current period, given current patient risk.

These models use data from the same time period as the outcome of interest. They are typically used for benchmarking and performance evaluation, helping to explain current variation by adjusting for patient complexity.

- **Use case:** Comparing actual vs. expected costs for a population in 2024 to assess efficiency or variation
- **Example:** Use 2024 data (age, conditions, etc.) to estimate what 2024 expenditures *should* have been

---

####  **Key Differences**

|                     | **Prospective Model**                   | **Concurrent Model**                   |
|---------------------|------------------------------------------|----------------------------------------|
| **Goal**            | Predict future outcomes                  | Benchmark current outcomes             |
| **Input data**      | Prior or current period                  | Current period only                    |
| **Output**          | Future risk score or expected outcome    | Expected value for current outcome     |
| **Common uses**     | Risk stratification, care management     | Risk-adjusted benchmarking, evaluation |
| **Example**         | Predicting 2025 cost from 2024 data      | Estimating expected 2024 cost in 2024  |

---

Both types of models play an important role in a comprehensive analytics strategy. Prospective models help organizations plan and act, while concurrent models help explain and improve performance.


## How to Interpret Benchmarking Results
---

Benchmarking models are designed to support population-level insights, not to make precise predictions for individual patients.

In general, these models are well-calibrated in aggregate. The total predicted cost or utilization across a large group closely matches the actual total. This is often reflected in a predicted-to-actual ratio near 100%.

However, at the individual level, these models typically have a high degree of prediction error. A common metric for this is Mean Absolute Error (MAE%), which often exceeds 60% for individual cost predictions.

#### Why this happens:

- Healthcare costs are highly variable at the individual level and influenced by rare or random events (e.g., emergency surgery, new diagnoses).
- Benchmarking models are built to capture average expected outcomes, not rare outliers.
- The goal is to produce fair comparisons across populations or provider groups.

#### What this means in practice:

- Use model results in aggregate: For example, to compare observed vs. expected PMPM across a provider group or patient cohort.
- Do not use model results to assess individual patients: A single patient’s predicted vs. actual cost may differ significantly, which is normal.

By focusing on population-level comparisons, benchmarking models enable organizations to identify variation, measure performance, and support accountability in value-based care, without requiring perfect individual prediction.


## Two Types of Benchmarking Models

### 1. Denominator-Based Models  
These models predict metrics across an entire population (the denominator):  
- Predict yearly or monthly spend across all members  
- Predict utilization rates across service categories  
- Examples: Total PMPM cost, inpatient admissions per 1,000 members

### 2. Encounter-Based Models  
These models predict outcomes for specific healthcare encounters that have already occurred:  
- Given a patient has an inpatient admission, what is their predicted chance of readmission?  
- Given a patient is admitted, what is their predicted length of stay?  
- Given an admission, what is the likelihood of discharge to home versus a skilled nursing facility?

Encounter-based metrics, particularly readmission rates, are valuable in population health management as they help identify opportunities for improved care transitions, follow-up care, and chronic condition management.

## Tuva's Risk-adjusted Benchmarking Models 

The Tuva Project has developed a set of benchmarking models using Medicare data from the CMS Limited Data Set (5% file). These models run on output from The Tuva Project's benchmarking mart, so no additional data transformations are needed to get started.

Visit [The Tuva Project's page on Hugging Face](https://huggingface.co/tuva-ml-models/) to get started integrating these models with your own data. The page also provides detailed information about each model, including evaluation metrics and performance insights.

In line with The Tuva Project's philosophy of openness and collaboration, we have initially made available models trained on CMS data. We encourage active participation from the community, inviting members to contribute their own models. Ultimately, our vision is to build a place where users can create and share machine learning models that leverage The Tuva Project’s data resources.

---

## Available Tuva Models

The following list of available models are available as both concurrent and prospective models trained on the 2023 CMS Limited Data Set (5% file) and are designed to run on the output of The Tuva Project's benchmarking data mart.

<br />

<details>
  <summary><b>Denominator-Based: Spend and Utilization Models</b></summary>
  
  <br />
  
  These models predict total paid amounts and encounter counts for a member population over a given period.

  **Paid Amount Model**
  * Total Paid Amount

  **Encounter Groups (Paid Amount and Encounter Counts)**
  * Inpatient
  * Outpatient
  * Office-based
  * Other

  **Encounter Count Models by Type (Paid Amount and Encounter Counts)**
  * Acute Inpatient
  * Ambulatory Surgery Center
  * Dialysis
  * DME - Orphaned
  * Emergency Department
  * Home Health
  * Inpatient Hospice
  * Inpatient Long Term Acute Care
  * Inpatient Psych
  * Inpatient Rehabilitation
  * Inpatient Skilled Nursing
  * Inpatient Substance Use
  * Lab - Orphaned
  * Office Visit
  * Office Visit - Injections
  * Office Visit - Other
  * Office Visit - PT/OT/ST
  * Office Visit - Radiology
  * Office Visit - Surgery
  * Orphaned Claim
  * Outpatient Hospital or Clinic
  * Outpatient Hospice
  * Outpatient Injections
  * Outpatient Psych
  * Outpatient PT/OT/ST
  * Outpatient Radiology
  * Outpatient Rehabilitation
  * Outpatient Substance Use
  * Outpatient Surgery
  * Telehealth
  * Urgent Care

</details>

<br />

<details>
  <summary><b>Encounter-Based: Acute Inpatient Models</b></summary>
  
  <br />

  Given that a member has had an acute inpatient encounter, these models predict specific outcomes for that admission.

  * **30-Day Readmission Risk:** The likelihood of a readmission within 30 days of discharge.
  * **Length of Stay (LOS):** The predicted number of days for the inpatient stay.
  * **Discharge Location:** The likelihood of being discharged to a specific setting (e.g., Home, Skilled Nursing Facility).

</details>