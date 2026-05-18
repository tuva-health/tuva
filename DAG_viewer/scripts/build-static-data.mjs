import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { cp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { gunzipSync } from "node:zlib";
import { fileURLToPath, pathToFileURL } from "node:url";

import { parse as parseYaml } from "yaml";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const dagViewerRoot = path.resolve(scriptDir, "..");
const publicRoot = path.join(dagViewerRoot, "public");
const distRoot = path.join(dagViewerRoot, "dist");
const cacheRoot = path.join(dagViewerRoot, ".cache");
const manifestPath = path.join(cacheRoot, "manifest.json");
const lineageCacheRoot = path.join(cacheRoot, "lineage");
const defaultRepoUrl = "https://github.com/tuva-health/tuva.git";
const defaultGithubRef = "main";
const staticSeedPreviewRowLimit = Number(process.env.TUVA_DAG_SEED_PREVIEW_ROW_LIMIT || 1000) || 1000;

async function main() {
  const sourceRoot = await resolveSourceRoot();
  await prepareDist();
  await buildLiteManifest(sourceRoot);
  await exportLineage(sourceRoot);
}

async function resolveSourceRoot() {
  if (process.env.TUVA_DAG_SOURCE_ROOT) {
    const sourceRoot = path.resolve(process.env.TUVA_DAG_SOURCE_ROOT);

    if (!existsSync(path.join(sourceRoot, "dbt_project.yml"))) {
      throw new Error(`TUVA_DAG_SOURCE_ROOT does not look like a Tuva checkout: ${sourceRoot}`);
    }

    return sourceRoot;
  }

  const repoUrl = process.env.TUVA_DAG_REPO_URL || defaultRepoUrl;
  const githubRef = process.env.TUVA_DAG_GITHUB_REF || defaultGithubRef;
  const sourceRoot = path.join(cacheRoot, "tuva-main");

  await rm(sourceRoot, { recursive: true, force: true });
  await mkdir(cacheRoot, { recursive: true });

  const result = spawnSync(
    "git",
    ["clone", "--depth=1", "--branch", githubRef, repoUrl, sourceRoot],
    {
      cwd: dagViewerRoot,
      encoding: "utf8"
    }
  );

  if (result.status !== 0) {
    throw new Error(result.stderr || result.stdout || `Unable to clone ${repoUrl}#${githubRef}`);
  }

  return sourceRoot;
}

async function prepareDist() {
  await rm(distRoot, { recursive: true, force: true });
  await mkdir(distRoot, { recursive: true });
  await cp(publicRoot, distRoot, { recursive: true });
  await mkdir(path.join(distRoot, "data"), { recursive: true });
}

async function buildLiteManifest(sourceRoot) {
  const files = await walkSelectedSourceFiles(sourceRoot);
  const modelDocsByName = await collectYamlDocs({ sourceRoot, files, collectionKey: "models" });
  const seedDocsByName = await collectYamlDocs({ sourceRoot, files, collectionKey: "seeds" });
  const modelFiles = files.filter((filePath) => filePath.startsWith("models/") && filePath.endsWith(".sql"));
  const seedFiles = files.filter((filePath) => filePath.startsWith("seeds/") && filePath.endsWith(".csv"));
  const nameToNodeId = new Map();

  for (const filePath of modelFiles) {
    nameToNodeId.set(path.basename(filePath, ".sql"), `model.the_tuva_project.${path.basename(filePath, ".sql")}`);
  }

  for (const filePath of seedFiles) {
    nameToNodeId.set(path.basename(filePath, ".csv"), `seed.the_tuva_project.${path.basename(filePath, ".csv")}`);
  }

  const nodes = {};

  for (const filePath of modelFiles) {
    const sql = await readFile(path.join(sourceRoot, filePath), "utf8");
    const name = path.basename(filePath, ".sql");
    const doc = modelDocsByName.get(name) || null;
    const uniqueId = nameToNodeId.get(name);

    nodes[uniqueId] = buildNode({
      name,
      uniqueId,
      filePath,
      resourceType: "model",
      doc,
      dependsOn: parseRefDependencies(sql, nameToNodeId),
      materialized: inferMaterialized(sql, doc?.entry)
    });
  }

  for (const filePath of seedFiles) {
    const name = path.basename(filePath, ".csv");
    const doc = seedDocsByName.get(name) || null;
    const uniqueId = nameToNodeId.get(name);

    nodes[uniqueId] = buildNode({
      name,
      uniqueId,
      filePath,
      resourceType: "seed",
      doc,
      dependsOn: [],
      materialized: "seed"
    });
  }

  await mkdir(path.dirname(manifestPath), { recursive: true });
  await writeFile(
    manifestPath,
    JSON.stringify(
      {
        metadata: {
          adapter_type: "static",
          project_name: "the_tuva_project",
          generated_at: new Date().toISOString()
        },
        nodes,
        sources: {}
      },
      null,
      2
    ),
    "utf8"
  );
}

function buildNode({ name, uniqueId, filePath, resourceType, doc, dependsOn, materialized }) {
  const entry = doc?.entry || {};
  const config = entry.config || {};
  const meta = {
    ...(entry.meta || {}),
    ...(config.meta || {})
  };

  return {
    unique_id: uniqueId,
    resource_type: resourceType,
    package_name: "the_tuva_project",
    name,
    alias: config.alias || name,
    schema: inferSchema(filePath, resourceType),
    path: filePath,
    original_file_path: filePath,
    patch_path: doc ? `the_tuva_project://${doc.filePath}` : null,
    description: entry.description || "",
    columns: buildManifestColumns(entry.columns || []),
    depends_on: {
      nodes: dependsOn
    },
    config: {
      ...config,
      materialized,
      meta
    },
    meta,
    tags: Array.isArray(entry.tags)
      ? entry.tags
      : Array.isArray(config.tags)
        ? config.tags
        : []
  };
}

function buildManifestColumns(columns) {
  return Object.fromEntries(
    columns
      .filter((column) => column?.name)
      .map((column) => [
        column.name,
        {
          name: column.name,
          description: column.description || "",
          data_type: column.data_type || column.config?.meta?.data_type || column.meta?.data_type || null,
          meta: column.meta || {},
          config: column.config || {}
        }
      ])
  );
}

function parseRefDependencies(sql, nameToNodeId) {
  const dependencies = new Set();
  const refPattern = /ref\s*\(\s*(?:(["'])[^"']+\1\s*,\s*)?(["'])([^"']+)\2/g;
  let match;

  while ((match = refPattern.exec(sql)) !== null) {
    const referencedName = match[3];
    const dependencyId = nameToNodeId.get(referencedName);

    if (dependencyId) {
      dependencies.add(dependencyId);
    }
  }

  return Array.from(dependencies).sort();
}

function inferMaterialized(sql, entry = {}) {
  const configured = entry.config?.materialized || entry.materialized;

  if (configured) {
    return configured;
  }

  const match = sql.match(/materialized\s*=\s*["']([^"']+)["']/i);
  return match?.[1] || "view";
}

function inferSchema(filePath, resourceType) {
  const parts = filePath.split("/");

  if (resourceType === "seed") {
    return parts[1] || "seed";
  }

  if (filePath.startsWith("models/input_layer/")) {
    return "input_layer";
  }

  if (filePath.startsWith("models/core/")) {
    return "core";
  }

  if (filePath.startsWith("models/data_marts/")) {
    return parts[2] || "data_marts";
  }

  if (filePath.startsWith("models/claims_preprocessing/")) {
    return "claims_preprocessing";
  }

  if (filePath.startsWith("models/normalization/")) {
    return "normalization";
  }

  return parts[1] || "model";
}

async function collectYamlDocs({ sourceRoot, files, collectionKey }) {
  const docsByName = new Map();
  const yamlFiles = files.filter((filePath) => /\.(ya?ml)$/i.test(filePath));

  for (const filePath of yamlFiles) {
    let document;

    try {
      document = parseYaml(await readFile(path.join(sourceRoot, filePath), "utf8")) || {};
    } catch {
      continue;
    }

    const entries = Array.isArray(document[collectionKey]) ? document[collectionKey] : [];

    for (const entry of entries) {
      if (!entry?.name || docsByName.has(entry.name)) {
        continue;
      }

      docsByName.set(entry.name, { filePath, entry });
    }
  }

  return docsByName;
}

async function exportLineage(sourceRoot) {
  process.env.DAG_REPO_ROOT = sourceRoot;
  process.env.DAG_MANIFEST_PATH = manifestPath;
  process.env.DAG_CACHE_DIR = lineageCacheRoot;

  const lineageModule = await import(pathToFileURL(path.join(scriptDir, "build-lineage.mjs")).href);
  const targets = await lineageModule.listTargetConfigs();
  const seedPreviewNodes = new Map();

  for (const target of targets) {
    const payload = await lineageModule.buildLineagePayload({ targetKey: target.key });
    const staticResponse = buildStaticResponse({ payload, targets });
    const outputPath = path.join(distRoot, "data", `${target.key}-lineage.json`);

    await writeFile(outputPath, `${JSON.stringify(staticResponse, null, 2)}\n`, "utf8");
    collectSeedPreviewNodes(seedPreviewNodes, payload);
  }

  await exportStaticSeedPreviews(seedPreviewNodes);
}

function buildStaticResponse({ payload, targets }) {
  const generatedAt = payload.generatedAt || new Date().toISOString();

  return {
    payload,
    targets,
    capabilities: {
      canEdit: false
    },
    refresh: {
      status: "ready",
      payloadVersion: 1,
      activeTargetKey: payload.target?.key || "system_overview",
      activeTrigger: "static_export",
      lastAttemptAt: generatedAt,
      lastSuccessAt: generatedAt,
      lastError: null,
      hasPayload: true,
      mode: "static"
    }
  };
}

function collectSeedPreviewNodes(seedPreviewNodes, payload) {
  for (const node of payload.nodes || []) {
    if (node?.resourceType !== "seed" || seedPreviewNodes.has(node.id)) {
      continue;
    }

    seedPreviewNodes.set(node.id, {
      nodeId: node.id,
      name: node.name,
      csvPath: node.paths?.sql || null,
      seedViewer: node.seedViewer || null,
      columns: node.columns || []
    });
  }
}

async function exportStaticSeedPreviews(seedPreviewNodes) {
  const outputRoot = path.join(distRoot, "data", "seed-previews");
  await mkdir(outputRoot, { recursive: true });

  for (const seedNode of seedPreviewNodes.values()) {
    const snapshot = await buildStaticSeedPreviewSnapshot(seedNode);
    const outputPath = path.join(outputRoot, `${seedNode.nodeId}.json`);
    await writeFile(outputPath, `${JSON.stringify(snapshot, null, 2)}\n`, "utf8");
  }
}

async function buildStaticSeedPreviewSnapshot(seedNode) {
  const fallbackHeaders = seedNode.columns.map((column) => column.name).filter(Boolean);
  let preview = null;
  let sourceError = null;

  if (seedNode.seedViewer?.downloadUrl) {
    try {
      preview = await readCsvPreviewFromUrl(seedNode.seedViewer.downloadUrl, staticSeedPreviewRowLimit, fallbackHeaders);
    } catch (error) {
      sourceError = error instanceof Error ? error.message : String(error);
    }
  }

  if (!preview && seedNode.csvPath && existsSync(seedNode.csvPath)) {
    try {
      preview = await readCsvPreviewFromFile(seedNode.csvPath, staticSeedPreviewRowLimit, fallbackHeaders);
    } catch (error) {
      sourceError = sourceError || (error instanceof Error ? error.message : String(error));
    }
  }

  return {
    nodeId: seedNode.nodeId,
    name: seedNode.name,
    generatedAt: new Date().toISOString(),
    sourceUrl: seedNode.seedViewer?.downloadUrl || null,
    sourceError,
    headers: preview?.headers?.length ? preview.headers : fallbackHeaders,
    rows: preview?.rows || [],
    cachedRows: preview?.rows?.length || 0,
    totalRows: preview?.totalRows ?? null,
    rowLimit: staticSeedPreviewRowLimit,
    truncated: Boolean(preview?.truncated)
  };
}

async function readCsvPreviewFromUrl(url, rowLimit, preferredHeaders = []) {
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`CSV source returned ${response.status}`);
  }

  const sourceBuffer = Buffer.from(await response.arrayBuffer());
  const csvBuffer = isGzipBuffer(sourceBuffer) ? gunzipSync(sourceBuffer) : sourceBuffer;
  return readCsvPreviewFromText(csvBuffer.toString("utf8"), rowLimit, preferredHeaders);
}

async function readCsvPreviewFromFile(filePath, rowLimit, preferredHeaders = []) {
  return readCsvPreviewFromText(await readFile(filePath, "utf8"), rowLimit, preferredHeaders);
}

function readCsvPreviewFromText(csvText, rowLimit, preferredHeaders = []) {
  const lines = csvText.split(/\r?\n/);
  const firstLine = lines.length ? parseCsvLine(lines[0].replace(/^\uFEFF/, "")) : [];
  const hasPreferredHeaders = preferredHeaders.length > 0;
  const headers = hasPreferredHeaders ? preferredHeaders : firstLine;
  const firstLineIsHeader = hasPreferredHeaders
    ? doCsvHeadersMatch(firstLine, preferredHeaders)
    : firstLine.length > 0;
  const dataLines = lines.slice(firstLineIsHeader ? 1 : 0).filter((line) => line.length);
  const rows = dataLines.slice(0, rowLimit).map(parseCsvLine);

  return {
    headers,
    rows,
    totalRows: dataLines.length,
    truncated: dataLines.length > rowLimit
  };
}

function doCsvHeadersMatch(csvHeaders, preferredHeaders) {
  if (csvHeaders.length !== preferredHeaders.length) {
    return false;
  }

  return csvHeaders.every((header, index) => normalizeCsvHeader(header) === normalizeCsvHeader(preferredHeaders[index]));
}

function normalizeCsvHeader(header) {
  return String(header || "").trim().toLowerCase();
}

function isGzipBuffer(buffer) {
  return buffer.length >= 2 && buffer[0] === 0x1f && buffer[1] === 0x8b;
}

function parseCsvLine(line) {
  const cells = [];
  let cell = "";
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const character = line[index];

    if (character === "\"") {
      if (inQuotes && line[index + 1] === "\"") {
        cell += "\"";
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (character === "," && !inQuotes) {
      cells.push(cell);
      cell = "";
      continue;
    }

    cell += character;
  }

  cells.push(cell);
  return cells;
}

async function walkSelectedSourceFiles(sourceRoot) {
  const roots = ["models", "seeds"];
  const files = [];

  for (const root of roots) {
    const absoluteRoot = path.join(sourceRoot, root);

    if (!existsSync(absoluteRoot)) {
      continue;
    }

    files.push(...await walk(absoluteRoot, sourceRoot));
  }

  return files.sort();
}

async function walk(currentPath, sourceRoot) {
  const entries = await readdir(currentPath, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const absolutePath = path.join(currentPath, entry.name);

    if (entry.isDirectory()) {
      files.push(...await walk(absolutePath, sourceRoot));
      continue;
    }

    if (entry.isFile()) {
      files.push(path.relative(sourceRoot, absolutePath).replace(/\\/g, "/"));
    }
  }

  return files;
}

main().catch((error) => {
  process.stderr.write(`${error.stack || error}\n`);
  process.exitCode = 1;
});
