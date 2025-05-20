## Using Integration Tests

1. Set the project subdirectory to “integration_tests” if using dbt cloud or change directory to "integration_tests" (`cd integration_tests`) if using CLI.
2. Choose a data source:
   1. To use synthetic demo data:
        -  Set use_synthetic_data to true
   3. To use your own data sources, update the vars in integration_tests/dbt_project.yml:
        - Set input_database and input_schema to your testing sources
4. Run `dbt deps`.
5. Run `dbt build`.