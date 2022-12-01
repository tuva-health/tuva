# The Tuva Project

## 🧰 What does this project do?

The Tuva Project is the open source data transformation layer for healthcare data.  For a detailed overview of what the project does and how it works, check out our [Knowledge Base](https://thetuvaproject.com/docs/intro).  For information on data models and to view the entire DAG check out our dbt [Docs](https://tuva-health.github.io/the_tuva_project/#!/overview/terminology).

## 🔌 Database Support

- Snowflake

## ✅ How to get started

### Step 1:  Pre-requisites

- **Database:**  This package creates and transforms data in a database called Tuva (see step 5 for more detail).
- **Dataset:**  Claims data is available in your warehouse and modeled after the [Tuva Claims Input Layer](https://thetuvaproject.com/docs/data-models/claims-input-layer).
- **dbt version**:  This package requires you to have dbt installed and a functional dbt project running on version `1.2.x`.

### Step 2:  Package Installation

Include the following in your `packages.yml`

```yaml
packages:
  - package: tuva-health/the_tuva_project
    version: 0.1.0
```

Please refer to [dbt Hub](https://hub.getdbt.com/) or read the [dbt docs](https://docs.getdbt.com/docs/build/packages) for the latest information on installing packages.

### Step 3:  Configure input

By default, this package will use the claims data created by your dbt project (i.e. the data in the target database and schema in your `dbt_project.yml`).  As long as the model names match the [Tuva Claim Data Model](https://thetuvaproject.com/docs/category/claims-data-model), no additional configuration is needed.

### Step 4:  Enabling and disabling packages

By default, all packages are enabled to create a comprehensive analytics platform.  If you would like to disable a package, the respective variables can be added to your `dbt_project.yml`.

```yaml
vars:
  tuva_packages_enabled: false         # by default true; toggle for all packages

  chronic_conditions_enabled: false    # by default true; toggle for specific package
  claims_preprocessing_enabled: false  # by default true; toggle for specific package
  data_profiling_enabled: false        # by default true; toggle for specific package
  readmissions_enabled: false          # by default true; toggle for specific package
  terminology_enabled: false           # by default true; toggle for specific package
```

### (Optional) Step 5:  Change build schema and database

By default, this package will build all models in a database called `Tuva` schema names reflect the package that created them.  This behavior can be altered at two grains:  for all packages or for each individual package.

Variables that start with `tuva` are configurations for all packages.  The database can be altered and a prefix added to the default schema.

Variables that start with the package name (e.g. `readmissions`) are configurations for just that package.  The database and schema can be altered for all models in that particular package.

These changes are made by adding the respective variables to your `dbt_project.yml`:

```yaml
vars:
  tuva_database: tuva                              # configuration for all packages
  tuva_schema_prefix: testing                      # configuration for all packages

  chronic_conditions_database: tuva                # configuration for the specific package
  chronic_conditions_schema: chronic_conditions    # configuration for the specific package
  claims_preprocessing_database: tuva              # configuration for the specific package
  claims_preprocessing_schema: core                # configuration for the specific package
  data_profiling_database: tuva                    # configuration for the specific package
  data_profiling_schema: data_profiling            # configuration for the specific package
  readmissions_database: tuva                      # configuration for the specific package
  readmissions_schema: readmissions                # configuration for the specific package
  terminology_database: tuva                       # configuration for the specific package
  terminology_schema: terminology                  # configuration for the specific package
```

### (Optional) Step 6:  Additional configurations
<details>
<summary> Expand for details </summary>
    
**Modifying a model alias, materialization, and tags**
    
All model-level configurations for a package are in `_models.yml`.  Only a few settings should be altered within this file:
    
- [Custom aliases](https://docs.getdbt.com/docs/build/custom-aliases) - An override of the model name, creating a clearer table name.
- [Tags](https://docs.getdbt.com/reference/resource-configs/tags) - A categorization and organization of models
- [Materialization](https://docs.getdbt.com/docs/build/materializations) - Pre-configure based on internal testing of query performance
    
> NOTE: The [enabled](https://docs.getdbt.com/reference/resource-configs/enabled) property has also been set within the model.sql file due to limitations with dbt.
>
</details>

## 🤹🏽 **Does this package have dependencies?**

This dbt package is dependent on the following dbt packages. For more information on the below packages, refer to the [dbt hub](https://hub.getdbt.com/) site.

> If you have any of these dependent packages in your own `packages.yml` we highly recommend removing them to ensure there are no package version conflicts.
> 

```
packages:
  - package: dbt-labs/dbt_utils
    version: [">=0.8.0", "<0.9.0"]
  - package: tuva-health/chronic_conditions
    version: [">=0.1.0"]
  - package: tuva-health/claims_preprocessing
    version: [">=0.1.0"]
  - package: tuva-health/data_profiling
    version: [">=0.1.0"]
  - package: tuva-health/readmissions
    version: [">=0.1.0"]
  - package: tuva-health/terminology
    version: [">=0.1.0"]
```

## 🙋🏻‍♀️ **How is this package maintained and can I contribute?**

### **Package Maintenance**

The Tuva Project team maintaining this package **only** maintains the latest version of the package. We highly recommend you stay consistent with the latest version.

### Contributions

Have an opinion on the mappings? Notice any bugs when installing and running the package?
If so, we highly encourage and welcome feedback!  While we work on a formal process in Github, we can be easily reached in our Slack community.

## 🤝 Community

Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
