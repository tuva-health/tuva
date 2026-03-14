---
id: building-a-connector
title: "Building a Connector"
---

To get started with a new data source in the Tuva Project, for which there is no pre-built connector, you must build a custom connector. In the context of The Tuva Project, a connector is a dbt project containing SQL models that *standardize* and *transform* raw data so it meets the expectations of the Tuva Input Layer. For example, this can include renaming columns to match Tuva column names, adjusting formats of the columns so that the values match the Input Layer and transforming the data when the expected values in input layer rows have a different *grain* (level of detail or uniqueness of each row of a table, or what each row represents) than the source data. 

![Connectors](/img/claim_id_standardization_image.png)

The image above shows an example of the following transformation steps that would all be performed within a connector:
1. **Column Names:** Source column names (like clm\_id or clm\_nr) must be renamed to align with the Tuva column names in the Input Layer (claim\_id).   
2. **Data Types:** Additionally, in the second source example, claim\_line\_number is stored as a string with a leading zero in the first position, but the Tuva Input Layer requires that it be stored as an integer, so the data type must be adjusted.   
3. **Logical Transformations:** Occasionally information in the source may be contained within fewer (or more) columns (or rows) than specified in the Input Layer. In this example, claim\_number in the source data is a concatenation of the value of claim\_number and the claim\_line\_number. The Tuva Input Layer requires claim\_number to be named claim\_id and to contain only the claim header ID. The claim\_line\_number must be extracted in the third data source and placed in the claim\_line\_number column.

This guide describes how to build a connector using our [Connector Template](https://github.com/tuva-health/connector_template).  The video below summarizes this.

<iframe 
width="600" 
height="400" 
src="https://www.youtube.com/embed/RC-o-HvZ5fc?si=8JNUnv7ezbPzWevb" 
title="YouTube video player" 
frameborder="0" 
allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" 
referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

## Step 1: Create a New Repo from the Connector Template

First, you need to perform some basic steps to get the connector template on your local machine.

1. Visit the [Connector](https://github.com/tuva-health/connector_template) repository on GitHub.
2. Click **"Use this template"** (see screenshot below).

![Connectors](/img/connector-template.png)

3. Name your new repository (e.g., `my-connector`).  We recommend choosing a name that aligns with your data source e.g. UHC_claims.
4. Click **"Create repository"**.  Your new repo will contain all template files with their Git history.
5. Clone the new repository and open it in a code editor of your choice

```bash
git clone https://github.com/your-username/my-connector.git
cd my-connector
```

## Step 2: Update `dbt_project.yml`

The `dbt_project.yml` is one of the most important files in a dbt project because it controls many aspects about how the project runs (e.g. what models run, where tables are built in the database, etc.).  

1. Rename the project:

```yaml
name: my_connector
```

2. Set model-level configs to use your project name:

```yaml
models:
  my_connector:
    ...
```

3. Enable the appropriate connector type depending on the type of data source you're working with.  In the example below we're working with a claims data source.  

```yaml
vars:
  claims_enabled: true
  clinical_enabled: false
```

4. (Optional) Set Tuva Variables.  Tuva ships with a number of Data Marts that use dbt variables to control their behavior.  You may need to set these variables.  For information on these variables see [dbt Variables](../dbt-variables.md).

## Step 3: Clean Up the Models

Inside `models/`, you’ll find:
- `staging/`
- `intermediate/`
- `final/`

The `staging/` and `intermediate/` directories are initially empty (aside from `.gitkeep` files). Inside these folders is where you'll create custom SQL files to trasform your raw data into the Input Layer.

In `models/sources.yml`, remove any models you don't intend to use. For example, if you are building a claims connector, you would keep only:

```yaml
- name: elibility
- name: medical_claim
- name: pharmacy_claim
```

Also clean up the corresponding `models/models.yml` file, removing unnecessary models, etc.

## Step 4: Create a Virtual Environment

Technically this step is optional, but it's a very good practice.  Create and activate a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows
```

Next, install required Python packages (including dbt-core and Snowflake adapter):

```bash
pip install -r requirements.txt
```

If needed, update `requirements.txt` to match your environment.

## Step 5: Configure and Test Your dbt Profile

Ensure your `profiles.yml` (in `~/.dbt/`) is correctly configured for Snowflake.

Test the connection:

```bash
dbt debug
```

## Step 6: Install dbt Dependencies

```bash
dbt deps
```

This installs any packages defined in `packages.yml` (such as `tuva`).

## Step 7: Begin Building Your Models

You now have a fully set-up connector repository, ready to convert raw claims data into Tuva’s Input Layer format. 
  - Your environment is set up
  - The connector template is configured for claims data

You’re ready to begin writing transformation logic in `staging/` and `intermediate/` folders.  For examples of how to structure the actual models you will use to standardize your data take a look at our Pre-Built Connectors.  

Mapping a data source to the Input Layer means creating dbt models in your dbt project for each of the Input Layer tables. That means that if you have a claims data source you will create dbt models for each of the claims tables, and if you have a clinical data source you will create dbt models for each of the clinical tables in the [Input Layer](../input-layer).

## Step 8: `dbt build`

Once you're satisified with your models you can execute `dbt build` to run the project.  This will attempt to build not only the models you created, but also run all Data Quality Tests, Core Data Model, Data Marts, and load all Terminology.
