# Input Layer Data Quality Assessment

This application provides an assessment of the data loaded into the input layer using the tests that are defined 
in The Tuva Project dbt package. It may pickup other tests if they are configured correctly.

To use this application, you must produce dbt artifacts `run_results.json` and `manifest.json` and load them into 
the web interface.

To produce these files:
1. Setup your input layer models.
2. Run your input models.
3. Run the input layer `dbt run -s input_layer`
4. Run `dbt test -s input_layer`
5. Load the artifacts into the web application.

# Input Layer Exploratory Charts

This application visualizes several metrics on the data in the input layer for easier 
assessment and validation.

To use this application, you must load a CSV file `exploratory_charts.csv`.

To produce this CSV file:
1. Setup your input layer models.
2. Run `dbt run -s +data_quality__exploratory_charts`
3. Find the table `data_quality.exploratory_charts` (the schema may have a prefix depending on your config)
4. Export that table to a CSV file. 
5. Copy that file into the `/ui/data` folder, or load it directly into the application using the `Choose File` button.

# Terminology Metrics (Percent Valid by data_source)

This page lets you quickly visualize key terminology validations (e.g., DRG, Bill Type, Revenue Center, HCPCS) as percentages per `data_source`.

To produce the CSV:
1. Ensure your input layer models are configured and run.
2. Run `dbt run -s +data_quality__terminology_metrics` to build the aggregated metrics table.
3. From your warehouse, export the table `data_quality.terminology_metrics` (actual schema may vary with your prefix/config) or the relation created by `data_quality__terminology_metrics` to a CSV file. This table contains only aggregated, non-PHI metrics.
4. Open `ui/input_layer_terminology_metrics.html` and drop the CSV file, or click to choose the file.

Notes:
- The CSV should include columns like: `data_source, metric_id, metric_name, claim_scope, denominator_n, valid_n, invalid_n, null_n, multiple_n, valid_pct, threshold, pass_flag`.
- No PHI: this workflow requires a manual export from your warehouse so implementers can validate what leaves their environment.

# Mart Review Dashboard

This page provides a PHI-safe dashboard for the `mart_review` checks and summaries. It expects one or more aggregated CSVs exported from your warehouse, grouped at minimum by `data_source` and, where applicable, by `payer` and `plan`. Do not export person-level rows.

Open: `ui/mart_review_dashboard.html`

Datasets supported (columns in parentheses):
- Age distribution: (`data_source`, `payer`, `plan`, `age_bucket`, `count`, `avg_age`)
- Gender distribution: (`data_source`, `payer`, `plan`, `gender`, `count`)
- PMPM by month: (`data_source`, `payer`, `plan`, `year_month`, `total_paid`, `medical_paid`, `pharmacy_paid`)
- Members with claims: (`data_source`, `year_month`, `members_with_claims`, `total_member_months`, `percent_members_with_claims`)
- Claims with enrollment: (`data_source`, `payer`, `plan`, `year_month`, `claims_with_enrollment`, `claims`, `percentage_claims_with_enrollment`)
- Chronic conditions: (`data_source`, `condition`, `count`)
- Inpatient LOS distribution: (`data_source`, `payer`, `plan`, `los_groups`, `count`)
- DRG frequency (optional): (`data_source`, `drg_code`, `count`)
- ED avoidable categories: (`data_source`, `avoidable_category`, `count`)
- Service categories: (`data_source`, `payer`, `plan`, `year_month`, `service_category_1`, `service_category_2`, `paid_amt`, `visits`)
- HCC code frequency: (`data_source`, `hcc_code`, `count`)
- Pharmacy pivot: (`data_source`, `payer`, `plan`, `theraclass` or `atc_3_name`, `ndc_code`, `ndc_description`, `spend`, `avg_days_supply`, `avg_quantity`, `avg_refills`)

To produce these CSVs, run your `mart_review` models and export aggregates from the corresponding relations under your data_quality schema (or the schema prefixed by `tuva_schema_prefix`). The dashboard includes copyable SQL templates tailored for aggregated outputs and will substitute your schema prefix for convenience.

PHI Guidance:
- Do not export person-level data (e.g., person_id, encounter_id, dates of birth). Use only aggregated counts, rates, and binned distributions.
- Where time is included, prefer `year_month` buckets (YYYYMM) rather than exact dates.
- When in doubt, aggregate more (e.g., top-N lists only, suppress small cells).

Single unified export (recommended):
- Build: `dbt run -s data_quality.dqi.mart_review.final.mart_review__dashboard_export` (adjust path/selector per your project)
- Export the table `data_quality.mart_review__dashboard_export` (schema may be prefixed by `tuva_schema_prefix`).
- Load that single CSV into `ui/mart_review_dashboard.html`. The page will detect the unified format and auto-populate all charts.
