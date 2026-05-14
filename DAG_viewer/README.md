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

To force a different source checkout or Git ref:

```bash
TUVA_DAG_SOURCE_ROOT=/path/to/tuva npm run build
TUVA_DAG_GITHUB_REF=main npm run build
```
