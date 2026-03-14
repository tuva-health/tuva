---
id: tuva-databricks
title: "Setting up Tuva on Databricks"
hide_title: true
---

# Setting up The Tuva Project on Databricks

<iframe width="560" height="315" src="https://www.youtube.com/embed/f074-6m2o3E?si=TrIqrB7xLbxao1H8" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

### Pre-requisites:

1. Access to a Databricks instance: dbt will write the outputs from this project to a catalog within this instance.
2. Git: you will need Git to clone the demo project repository.
3. Authentication Details to Databricks: depending on the authentication method you choose, you’ll need to supply dbt with connection details in a profiles.yml. For more information on connections, see the [dbt docs](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup?tokenoauth=token#examples).

### Step 1: Cloning the demo project

Open your terminal and clone the demo project.

```bash
git clone https://github.com/tuva-health/demo
cd demo
``` 
### Step 2:  Create and Activate a Virtual Environment

Creating a Python virtual environment will help you manage project dependencies.

1. Create a virtual environment inside the `demo` directory:

```bash
# use python3 if python defaults to Python 2
python -m venv venv
```

After you run this, you can run `ls` inside the demo directory. If creating the virtual environment worked, you’ll see a `venv` directory.

1. Next, you’ll want to activate the virtual environment:
- macOS / Linux (bash/zsh): `source venv/bin/activate`
- Windows (Command Prompt): `venv\Scripts\activate.bat`
- Windows (PowerShell): `.\venv\Scripts\Activate.ps1`
- Windows (Git Bash): `source venv/Scripts/activate`

If this went well, you’ll see (venv) prepended to your command prompt, like this:

`(venv) user@Users-Device demo %`

### Step 3: Install Python dependencies

For a databricks project, you’ll need to install `dbt-core` and `dbt-databricks`, the Databricks-specific dbt adapter.

```bash
pip install dbt-core dbt-databricks
```

### Step 4: Configure profiles.yml for a dbt-to-Databricks connection

- `profiles.yml` file location: By default, dbt looks for a profiles file at the path `~/.dbt/profiles.yml`.
- `profiles.yml` file contents: dbt offers two ways to connect to Databricks; the first is with a Personal Access Token (PAT), and the second is with an OAuth client. The [dbt docs](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup?tokenoauth=token#examples) provide some helpful examples. Below is an example config for Personal Access Token (PAT).

```yaml
your_databricks_profile_name:
  target: dev
  outputs:
    dev:
      type: databricks
      schema: schema_name # required
      host: YOURORG.databrickshost.com
      http_path: /SQL/HTTP/PATH/TO/YOUR/WAREHOUSE
      token: # token required if you're using PAT. Treat this like a password.
      catalog: dev # corresponds to the catalog where your data will be written
      threads: 1 # optional, defaults to 1
```

### Step 5: Install dbt package dependencies

The demo project is configured to import The Tuva Project and any relevant dependencies for running the Tuva Project on synthetic data. This import is done from the project directory (`demo`) with the following command:

```bash
dbt deps
```

This will read the packages specified in `packages.yml` and will populate the `dbt_packages/` directory with the necessary code.

### Step 6: Test the dbt-to-Databricks connection

Before running transformations, verify that dbt can connect to Snowflake using your profiles.yml settings:

```bash
dbt debug
```

Look for "Connection test: OK connection ok". If you see errors, double-check your profiles.yml settings (account, user, role, warehouse, authentication details, paths). Once you see that your connection is working, you are ready to run the project.

### Running the Project

Once setup is complete, you can run the dbt transformations:

Full Run (Recommended First Time), this command will:

- Run all models (.sql files in models/).
- Run all tests (.yml, .sql files in tests/).
- Materialize tables/views in your target data warehouse as configured.

```
dbt build
```

This might take some time depending on the data volume and warehouse size.

**Run Only Models:**

If you only want to execute the transformations without running tests:

```
dbt run
```

**Run Only Tests:**

To execute only the data quality tests:

`dbt test`
