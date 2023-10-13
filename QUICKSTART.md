# 🏁 Quickstart

There are 3 main ways to use the Tuva Project in your healthcare data warehouse.

- All Data Marts
- Single Data Mart
- Terminology Only


## Installing the dbt package

*Pre-requisite: In order to run the Tuva Project you need dbt installed and 
healthcare data loaded inside a data warehouse that we support. See the README 
for details.*

1. Add the Tuva Project to your `packages.yml`, or create a packages.yml if you 
   do not already have one. This should be created in the root of your dbt 
   project.
    ```yml
    packages:
      - package: tuva-health/the_tuva_project
        version: [">=0.5.0","<1.0.0"]
    ```
2. Import the package. 
    ```console
    dbt deps
    ```

## All Data Marts
The main difference between running a single data mart and all data marts is 
the mapping.  To run all data marts you need to map to the entire 
[Claims Data Model](https://tuva-health.github.io/the_tuva_project/#!/overview/input_layer).
*Stay tuned for details on the Clinical Data Model.*

The process of mapping an entire data model can be tricky.  If you need help mapping your data, 
feel free to post in [#buildersask](https://thetuvaproject.slack.com/archives/C03DET9ETK3) on Slack.

1. **Map to the input layer:** Map your data to the Claims Data Model. 
   The package expects you to have the following models in your dbt project: 
   `eligibility`, `medical_claim`, and `pharmacy_claim`. These will be referenced
   by the Core staging layer to run the Tuva Project.
2. **Set the variables:** You need to enable all models related to the type of 
   healthcare 
   data being used. Do this by adding the data source type variable to your 
   `dbt_project.yml` file and set the value to "true". Example: 
    ```yml
      vars:
        claims_enabled: true
3. Run the package to create all the models.
    ```console
    dbt build --select the_tuva_project 
    ```

### Other Variables
The Tuva Project relies on variables to set default behavior for the data marts.
These defaults can be found in the package's [dbt_project.yml](./dbt_project.yml).
You can change these values here or set them in the `dbt_project.yml` of your project.

* **tuva_last_run:** The date and timestamp of the dbt run that will populate 
  the tuva_last_run column in all models. Default timezone is UTC.
* **cms_hcc_payment_year:** The payment year for the CMS HCC mart. Defaults to 
  the current year.
* **cms_hcc_model_version:** The risk model used for the CMS HCC mart.
* **quality_measures_period_end:** The reporting date used to calculate the 
  performance periods for Quality Measures. Defaults to the current date.
* **snapshots_enabled:** Some data marts use the [dbt snapshot](https://docs.getdbt.com/docs/build/snapshots)
  feature. These are disabled by default. To enable them add this variable and 
  set the value to true (`snapshots_enabled: true`).

Alternatively, you can set these in the CLI. Example:
```console
dbt build --select the_tuva_project --vars '{cms_hcc_payment_year: 2020}'
```

### Connectors 
If your data source is a standard format that we have a connector 
for, you can use a connector to map your data.  A connector is just a dbt package 
that maps a standard data format to the Claims Data Model.  We currently have 
connectors for the following standard data formats:
- [Medicare LDS](https://github.com/tuva-health/medicare_saf_connector)
- [Medicare CCLF](https://github.com/tuva-health/medicare_cclf_connector)

## Single Data Mart
Every data mart in the Tuva Project can be run individually.  To run a single 
data mart complete the following steps.

1. **Map to staging:** Each data mart has its own staging layer.  The staging 
   layer is the set of models you need to create in order to run the data mart.
   You can find this in the `staging` folder under the data mart.
2. **Set the variables:** You need to enable the data mart you want to run.  Do this by 
   adding the variable for that data mart to your `dbt_project.yml` file and set 
   the value to "true".
   Example: 
    ```yml
      vars:
        cms_chronic_conditions_enabled: true
   ```
3. Run the package to build the marts you enabled.
    ```console
    dbt build --select the_tuva_project 
    ```

See [integration_tests](./integration_tests/dbt_project.yml) 
for more data mart variable examples.

## Terminology Only
You can disable the data marts and just load the terminology seeds by completing 
the following steps.

1. Add the variable `tuva_marts_enabled` to your `dbt_project.yml` file and set 
   the value to "false".
   ```yml
      vars:
        tuva_marts_enabled: false
   ```
2. Run the package to build the seeds.
   ```console
    dbt seed 
    ```

Alternatively, you can load all the terminology sets via SQL directly to your 
database. Check out the SQL for doing this [here](terminology_sql).
<br/><br/> 

## Tutorial

We created this [demo project](https://github.com/tuva-health/the_tuva_project_demo) 
to make it easy for people to explore the Tuva Project, even if you don't have 
access to healthcare data.

The demo includes:

- A 1,000 patient synthetic claims dataset based on Medicare claims data.
- All the data marts in the Tuva project.
- All the terminology sets in the Tuva Project.
<br/><br/> 