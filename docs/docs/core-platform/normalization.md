---
title: "3. Normalization"
github_path: "/tuva/models/normalization"
hide_table_of_contents: true
---

The Normalization Layer is a set of models that runs immediately after Data Quality. Its goal is to clean common, high-impact issues in Input Layer (see below for examples). Ideally these issues are addressed when the user maps data to the Input Layer, however, Normalization Layer serves as an additional check/guardrail.

The normalization layer includes:

- **Casting Data Types:** Data Quality tests surface data-type issues in the Input Layer. In the Normalization Layer, Tuva attempts to automatically cast and handle those values where possible.

- **Synthetic Key Creation:** Tuva creates synthetic keys that combine multiple columns (for example person, member, payer, and data source context) to make downstream analytics and joins easier.

- **Basic Code Cleaning:** Tuva applies standard cleaning patterns such as removing decimal places from certain code fields and padding missing leading zeros for fields like discharge disposition, bill type, and revenue center.

- **Voting for Single-Value Fields:** Some fields should have one value per claim (for example bill type). If multiple values exist, Tuva applies a basic voting approach and uses the most frequent value for that claim.
