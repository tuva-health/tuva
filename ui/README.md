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
