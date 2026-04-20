---
id: cms-aco-dashboards
title: "CMS ACO Dashboards"
hide_title: true
---

# CMS MSSP ACO Dashboards

The CMS MSSP ACO dashboards are published as Power BI template files (`.pbit`) in the [Tuva dashboards repository](https://github.com/tuva-health/dashboards). The setup has two parts:

1. **Semantic layer** — a shared Power BI semantic model that connects to your Snowflake data and is published to the Power BI Service
2. **ACO performance dashboard** — a report template that connects to the published semantic layer via a live connection

## Prerequisites

- Power BI Desktop installed
- Access to your Snowflake account with the Tuva data model deployed
- A Power BI workspace where you can publish datasets and reports

## Step 1: Download the template files

From the [Tuva dashboards repository](https://github.com/tuva-health/dashboards), download both template files:

- `power_bi/semantic_layer/enterprise_analytics_semantic_layer_snowflake.pbit`
- `power_bi/mssp_aco_dashboard/mssp_aco_performance.pbit`

To download an individual file from GitHub, navigate to the file, click the **Raw** button, then use **File > Save As** in your browser.

## Step 2: Set up and publish the semantic layer

The semantic layer is a standalone Power BI semantic model that connects to your Snowflake database. It must be published to the Power BI Service before the ACO dashboard can use it.

1. Open `enterprise_analytics_semantic_layer_snowflake.pbit` in Power BI Desktop.

2. When prompted, enter your connection parameters:

   | Parameter | Description |
   |-----------|-------------|
   | `snowflake_account` | Your Snowflake account identifier (e.g. `xy12345.us-east-1`) |
   | `snowflake_database` | The database where Tuva data is stored |
   | `snowflake_schema` | The schema containing the Tuva marts |
   | `snowflake_warehouse` | The Snowflake virtual warehouse to use |

3. Power BI Desktop will connect to Snowflake and load the data. Sign in with your Snowflake credentials when prompted.

4. Once loaded, publish the semantic model to your Power BI workspace: **Home > Publish**, then select your target workspace.

   Note the **workspace** and **semantic model name** — you will need these in Step 4.

## Step 3: Open the ACO performance dashboard template

1. Open `mssp_aco_performance.pbit` in Power BI Desktop.

2. The template will open and prompt you to connect to a data source. Since this report uses a live connection to the published semantic layer (rather than importing data itself), proceed to Step 4 to point it to your published model.

## Step 4: Connect the dashboard to the published semantic layer

The ACO performance dashboard uses a **live connection** to the semantic layer you published in Step 2. To configure this:

1. In Power BI Desktop, go to **Home > Transform data > Data source settings**.

2. Select the existing data source and click **Change Source**.

3. Under **Power BI datasets**, select the workspace and semantic model you published in Step 2.

4. Click **OK** and apply changes. The report visuals will now pull from your published semantic layer.

## Step 5: Publish the dashboard

Once the live connection is configured and the report loads correctly:

1. Go to **Home > Publish** in Power BI Desktop.
2. Select the same workspace where you published the semantic layer.
3. The ACO performance dashboard is now available in the Power BI Service.

## Accessing the dashboard in Power BI Service

After publishing:

- Navigate to your workspace in [app.powerbi.com](https://app.powerbi.com)
- You will see both the **semantic model** and the **ACO performance report** listed
- Open the report to view the dashboard; it will query Snowflake live via the semantic layer
