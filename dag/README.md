# Tuva DAG Prototype

Thin local web app for exploring a focused dbt lineage slice from Tuva.

## What it does

- Reads the current appointment lineage from `integration_tests/target/manifest.json`
- Merges available model metadata from the corresponding YAML files
- Reads the source SQL for each node
- Renders a focused DAG and a scrollable inspector for the selected node

## Run locally

```bash
cd /Users/aaronneiderhiser/code/tuva/dag
npm install
npm run dev
```

Then open the local URL printed by the server. The app will start a background refresh on boot and you can also use the in-app `Refresh DAG` button at any time.

## Current scope

This first pass focuses on the appointment path:

`appointment -> input_layer__appointment -> core__appointment`

The UI is intentionally scoped to one lineage slice so the design and metadata model can be refined before expanding to arbitrary model selections.

## Refresh workflow

- `GET /api/lineage` returns the current focused DAG payload plus refresh state.
- `POST /api/refresh` reruns `scripts/dbt-local parse` and rebuilds the payload.
- `GET /api/events` streams refresh lifecycle updates so the browser can update in place.

If refresh fails, the last successful DAG stays visible and the error details appear inline in the app.
