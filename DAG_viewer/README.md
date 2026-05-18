# Tuva DAG Viewer

Static DAG viewer for the Tuva dbt package.

The production site should be configured in Netlify as a separate site with:

- Base directory: `DAG_viewer`
- Build command: `npm ci && npm run build`
- Publish directory: `dist`
- Custom domain: `dagviewer.thetuvaproject.com`
- Branch: `main`

Do not configure an ignore rule that skips builds when only `models/` or
`seeds/` change. The viewer intentionally rebuilds from those main-branch files.

By default the build clones `tuva-health/tuva` at `main`, builds a lightweight
dbt manifest from the model YAML, seed YAML, SQL, and CSV files, then exports
static lineage JSON for every DAG target. When Netlify rebuilds the site after
changes land on `main`, the hosted viewer refreshes from the latest main-branch
YAML and SQL.

For local development against the current checkout:

```bash
npm install
npm run build:local
npm run serve
```

For local editing mode, use the dev server instead:

```bash
npm install
npm run dev
```

The dev server listens on `127.0.0.1:8000`, serves the app in live mode, and
enables the modal `Edit`/`Save` controls. Saves write back to the checked-out dbt
SQL and YAML files and run `scripts/dbt-local parse` so the local DAG reflects
dependency changes. The committed `public/index.html` remains hard-coded to
static mode, and the Netlify build publishes only `dist`, so edit mode is not
available on the public site.

To force a different source checkout or Git ref:

```bash
TUVA_DAG_SOURCE_ROOT=/path/to/tuva npm run build
TUVA_DAG_GITHUB_REF=main npm run build
```
