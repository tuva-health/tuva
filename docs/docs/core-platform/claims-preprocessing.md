---
title: "4. Claims Preprocessing"
github_path: "/tuva/models/claims_preprocessing"
hide_table_of_contents: true
---

Claims preprocessing is the step that makes claims data analytically usable before the Core Data Model is built. In practice, it is very difficult to do meaningful claims analytics without this preprocessing layer.

The Claims Preprocessing layer handles three foundational tasks:

- **Service categories:** Assign every claim and claim line to a service category that represents the type of service and care setting.
- **Encounter grouping:** Group claims into encounters (visits). An encounter means a distinct visit in a distinct care setting (for example acute inpatient, emergency department, skilled nursing facility, and office visits). Most encounters are made up of multiple claims, so grouping is required to analyze utilization and outcomes at the visit level.
- **Member months:** Calculate member months so population-normalized metrics can be produced (for example spend per 1,000 members per month or ED visits per 1,000 members).

These outputs are produced ahead of the Core Data Model and are used throughout the rest of the platform.
