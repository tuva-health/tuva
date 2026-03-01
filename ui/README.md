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