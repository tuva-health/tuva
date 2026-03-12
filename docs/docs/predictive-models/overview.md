---
id: overview
title: "Overview"
hide_title: true
---

# 9. Predictive Models
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 06-21-2025</em></small>
</div>

The Tuva Project is building a large library of predictive models that run on top of the Tuva data model.  These models include classical machine learning models, such as logistic regression, random forrest, and xg-boost.  

All the predictive models we are building fall into two categories:

1. Risk-adjusted Benchmarking Models
2. Risk Stratification Models

**Risk-adjusted Benchmarking Models** 
These models are used to estimate what should have happened, given a patient’s clinical and demographic profile. They are used to create "expected values" for outcomes like cost, utilization, or quality metrics and they are adjusted for patient-level risk factors.

For example, every patient in a claims dataset has an observed medical cost (e.g., PMPM). A risk-adjusted benchmarking model estimates the expected PMPM based on factors like age, comorbidities, and other clinical history. This allows you to fairly compare providers, health systems, or populations by controlling for differences in patient mix.

Think of it as answering:

“Given this patient’s risk profile, what would we expect their cost/utilization/outcome to be?”
You can then compare actual outcomes to these expected values to identify variation that may be due to practice patterns, quality of care, or other non-risk factors.


**Risk Stratification Models** 
These models are used to predict what will or might happen in the future. They estimate the probability of a specific event occurring, such as a hospital admission, ED visit, or high-cost status.

The goal is to rank or prioritize patients based on their future risk, so that care teams can intervene proactively.

For example, a stratification model might estimate the probability that a patient will be hospitalized in the next 30 days. Patients can then be sorted from highest to lowest predicted risk and flagged for care management or outreach.

Think of it as answering:

“Who is most likely to experience a costly or adverse outcome, so we can act now to prevent it?”