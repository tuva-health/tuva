import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { existsSync } from "node:fs";
import { readFile, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { gunzipSync } from "node:zlib";

import { isMap, isSeq, parseDocument } from "yaml";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const dagViewerRoot = path.resolve(scriptDir, "..");
const repoRoot = path.resolve(dagViewerRoot, "..");
const publicRoot = path.join(dagViewerRoot, "public");
const distRoot = path.join(dagViewerRoot, "dist");
const port = Number(process.env.PORT || process.env.DAG_VIEWER_DEV_PORT || 8000);
const host = process.env.DAG_VIEWER_DEV_HOST || "127.0.0.1";
const seedPreviewRowLimit = Number(process.env.TUVA_DAG_SEED_PREVIEW_ROW_LIMIT || 10000) || 10000;

process.env.DAG_REPO_ROOT ||= repoRoot;
process.env.DAG_MANIFEST_PATH ||= path.join(repoRoot, "integration_tests", "target", "manifest.json");
process.env.DAG_CACHE_DIR ||= path.join(dagViewerRoot, ".cache", "dev-lineage");

let lineageModule = null;
let payloadVersion = 1;
let refreshState = createRefreshState({
  status: "ready",
  activeTargetKey: "system_overview",
  activeTrigger: "dev_server"
});
const eventClients = new Set();

async function main() {
  lineageModule = await import(pathToFileURL(path.join(scriptDir, "build-lineage.mjs")).href);

  const server = createServer((request, response) => {
    void handleRequest(request, response).catch((error) => {
      const statusCode = error?.statusCode || 500;

      sendJson(request, response, statusCode, {
        error: statusCode === 500 ? "Internal server error" : error.message,
        detail: error instanceof Error ? error.stack || error.message : String(error)
      });
    });
  });

  server.listen(port, host, () => {
    process.stdout.write(`Tuva DAG Viewer dev server: http://${host}:${port}/\n`);
    process.stdout.write("Edit mode is enabled only through this localhost server.\n");
  });
}

async function handleRequest(request, response) {
  if (!isLoopbackRequest(request)) {
    sendJson(request, response, 403, { error: "The DAG Viewer dev server only accepts loopback requests." });
    return;
  }

  if (request.method === "OPTIONS") {
    writeCorsHeaders(request, response);
    response.writeHead(204);
    response.end();
    return;
  }

  const requestUrl = new URL(request.url || "/", `http://${request.headers.host || `${host}:${port}`}`);

  if (requestUrl.pathname === "/api/dag/events" && request.method === "GET") {
    handleEvents(request, response);
    return;
  }

  if (requestUrl.pathname === "/api/dag/lineage" && request.method === "GET") {
    const targetKey = requestUrl.searchParams.get("targetKey") || "system_overview";
    sendJson(request, response, 200, await buildLineageResponse(targetKey));
    return;
  }

  if (requestUrl.pathname === "/api/dag/refresh" && request.method === "POST") {
    const body = await readJsonBody(request);
    const targetKey = body.targetKey || "system_overview";
    sendJson(request, response, 200, await refreshLineage(targetKey, "manual_refresh"));
    return;
  }

  if (requestUrl.pathname === "/api/dag/save-node" && request.method === "POST") {
    const body = await readJsonBody(request);
    sendJson(request, response, 200, await saveNodeEdits(body));
    return;
  }

  if (requestUrl.pathname === "/api/dag/seed-preview" && request.method === "GET") {
    const targetKey = requestUrl.searchParams.get("targetKey") || "system_overview";
    const nodeId = requestUrl.searchParams.get("nodeId") || "";
    const query = requestUrl.searchParams.get("query") || "";
    const page = Number(requestUrl.searchParams.get("page") || 1) || 1;
    const pageSize = Number(requestUrl.searchParams.get("pageSize") || 50) || 50;

    sendJson(
      request,
      response,
      200,
      await buildSeedPreviewResponse({
        targetKey,
        nodeId,
        query,
        page,
        pageSize
      })
    );
    return;
  }

  await serveStaticAsset(request, response, requestUrl);
}

async function saveNodeEdits(body) {
  const targetKey = body?.targetKey || "system_overview";
  const nodeId = body?.nodeId || "";
  const changes = body?.changes || {};
  const payload = await lineageModule.buildLineagePayload({ targetKey });
  const node = payload.nodes.find((candidate) => candidate.id === nodeId);

  if (!node) {
    throw createHttpError(404, `No DAG node found for ${nodeId}`);
  }

  if (node.resourceType === "dag") {
    throw createHttpError(400, "Collapsed DAG boundary nodes cannot be edited.");
  }

  const changedFiles = [];

  if (Object.hasOwn(changes, "sql")) {
    const sqlPath = assertEditableSqlPath(node.paths?.sql);
    const nextSql = String(changes.sql || "");
    const currentSql = await readFile(sqlPath, "utf8");

    if (nextSql !== currentSql) {
      await writeFile(sqlPath, nextSql, "utf8");
      changedFiles.push(path.relative(repoRoot, sqlPath));
    }
  }

  if (hasYamlChanges(changes)) {
    const yamlPath = assertEditableYamlPath(node.paths?.yaml);
    const didUpdateYaml = await updateYamlNodeDocumentation({
      yamlPath,
      collectionKey: node.paths?.yamlCollectionKey || (node.resourceType === "seed" ? "seeds" : "models"),
      entryName: node.paths?.yamlEntryName || node.name,
      changes
    });

    if (didUpdateYaml) {
      changedFiles.push(path.relative(repoRoot, yamlPath));
    }
  }

  if (changedFiles.length) {
    await runDbtParse(targetKey, "save_node");
  }

  const response = await buildLineageResponse(targetKey);
  response.changedFiles = changedFiles;
  broadcastEvent("lineage_updated", { targetKey, changedFiles });
  return response;
}

async function refreshLineage(targetKey, trigger) {
  await runDbtParse(targetKey, trigger);
  const response = await buildLineageResponse(targetKey);
  broadcastEvent("lineage_updated", { targetKey });
  return response;
}

async function runDbtParse(targetKey, trigger) {
  refreshState = createRefreshState({
    status: "refreshing",
    activeTargetKey: targetKey,
    activeTrigger: trigger,
    lastAttemptAt: new Date().toISOString()
  });
  broadcastEvent("refresh_state", refreshState);

  try {
    const result = await runCommand(path.join(repoRoot, "scripts", "dbt-local"), ["parse"], {
      cwd: repoRoot,
      env: process.env
    });

    payloadVersion += 1;
    refreshState = createRefreshState({
      status: "ready",
      payloadVersion,
      activeTargetKey: targetKey,
      activeTrigger: trigger,
      lastAttemptAt: refreshState.lastAttemptAt,
      lastSuccessAt: new Date().toISOString(),
      lastCommandOutput: result.output
    });
    broadcastEvent("refresh_state", refreshState);
  } catch (error) {
    refreshState = createRefreshState({
      status: "failed",
      payloadVersion,
      activeTargetKey: targetKey,
      activeTrigger: trigger,
      lastAttemptAt: refreshState.lastAttemptAt,
      lastError: normalizeCommandError(error)
    });
    broadcastEvent("refresh_state", refreshState);
    throw error;
  }
}

function runCommand(command, args, options) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      env: options.env,
      shell: false
    });
    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", reject);
    child.on("close", (exitCode) => {
      const output = [stdout.trim(), stderr.trim()].filter(Boolean).join("\n\n");

      if (exitCode === 0) {
        resolve({ exitCode, output });
        return;
      }

      const error = new Error(`Command failed with exit code ${exitCode}: ${command} ${args.join(" ")}`);
      error.command = `${command} ${args.join(" ")}`;
      error.exitCode = exitCode;
      error.output = output;
      reject(error);
    });
  });
}

async function buildLineageResponse(targetKey) {
  const [payload, targets] = await Promise.all([
    lineageModule.buildLineagePayload({ targetKey }),
    lineageModule.listTargetConfigs()
  ]);
  const generatedAt = payload.generatedAt || new Date().toISOString();

  refreshState = {
    ...refreshState,
    activeTargetKey: payload.target?.key || targetKey,
    hasPayload: true,
    lastSuccessAt: refreshState.lastSuccessAt || generatedAt
  };

  return {
    payload,
    targets,
    capabilities: {
      canEdit: true
    },
    refresh: refreshState
  };
}

async function buildSeedPreviewResponse({ targetKey, nodeId, query, page, pageSize }) {
  const payload = await lineageModule.buildLineagePayload({ targetKey });
  const node = payload.nodes.find((candidate) => candidate.id === nodeId);

  if (!node || node.resourceType !== "seed") {
    throw createHttpError(404, `No seed node found for ${nodeId}`);
  }

  const fallbackHeaders = node.columns.map((column) => column.name).filter(Boolean);
  const preview = await readSeedPreviewSource(node, fallbackHeaders);
  const normalizedQuery = query.trim().toLowerCase();
  const matchingRows = normalizedQuery
    ? preview.rows.filter((row) => row.some((cell) => String(cell || "").toLowerCase().includes(normalizedQuery)))
    : preview.rows;
  const safePageSize = Math.max(1, Math.min(pageSize, 500));
  const safePage = Math.max(1, page);
  const startIndex = (safePage - 1) * safePageSize;

  return {
    nodeId,
    query,
    page: safePage,
    pageSize: safePageSize,
    totalRows: preview.totalRows,
    totalMatches: matchingRows.length,
    headers: preview.headers,
    rows: matchingRows.slice(startIndex, startIndex + safePageSize)
  };
}

async function readSeedPreviewSource(node, fallbackHeaders) {
  if (node.seedViewer?.downloadUrl) {
    const response = await fetch(node.seedViewer.downloadUrl);

    if (response.ok) {
      const sourceBuffer = Buffer.from(await response.arrayBuffer());
      const csvBuffer = isGzipBuffer(sourceBuffer) ? gunzipSync(sourceBuffer) : sourceBuffer;
      return readCsvPreviewFromText(csvBuffer.toString("utf8"), seedPreviewRowLimit, fallbackHeaders);
    }
  }

  const csvPath = assertReadableRepoPath(node.paths?.sql);

  return readCsvPreviewFromText(await readFile(csvPath, "utf8"), seedPreviewRowLimit, fallbackHeaders);
}

function readCsvPreviewFromText(csvText, rowLimit, preferredHeaders = []) {
  const lines = csvText.split(/\r?\n/);
  const firstLine = lines.length ? parseCsvLine(lines[0].replace(/^\uFEFF/, "")) : [];
  const hasPreferredHeaders = preferredHeaders.length > 0;
  const headers = hasPreferredHeaders ? preferredHeaders : firstLine;
  const firstLineIsHeader = hasPreferredHeaders
    ? headersMatch(firstLine, preferredHeaders)
    : firstLine.length > 0;
  const dataLines = lines.slice(firstLineIsHeader ? 1 : 0).filter((line) => line.length);
  const rows = dataLines.slice(0, rowLimit).map(parseCsvLine);

  return {
    headers,
    rows,
    totalRows: dataLines.length
  };
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

function headersMatch(csvHeaders, preferredHeaders) {
  if (csvHeaders.length !== preferredHeaders.length) {
    return false;
  }

  return csvHeaders.every((header, index) => normalizeCsvHeader(header) === normalizeCsvHeader(preferredHeaders[index]));
}

function normalizeCsvHeader(value) {
  return String(value || "").trim().toLowerCase();
}

function isGzipBuffer(buffer) {
  return buffer.length >= 2 && buffer[0] === 0x1f && buffer[1] === 0x8b;
}

async function updateYamlNodeDocumentation({ yamlPath, collectionKey, entryName, changes }) {
  const originalYaml = await readFile(yamlPath, "utf8");
  const document = parseDocument(originalYaml, {
    keepSourceTokens: true
  });

  if (document.errors.length) {
    throw createHttpError(
      400,
      `Unable to parse ${path.relative(repoRoot, yamlPath)}: ${document.errors.map((error) => error.message).join("; ")}`
    );
  }

  const collection = document.get(collectionKey, true);

  if (!isSeq(collection)) {
    throw createHttpError(400, `${path.relative(repoRoot, yamlPath)} does not contain a ${collectionKey} list.`);
  }

  const entry = collection.items.find((item) => isMap(item) && item.get("name") === entryName);

  if (!entry) {
    throw createHttpError(404, `No ${collectionKey} entry named ${entryName} in ${path.relative(repoRoot, yamlPath)}.`);
  }

  if (Object.hasOwn(changes, "description")) {
    entry.set("description", String(changes.description || "").trimEnd());
  }

  if (Object.hasOwn(changes, "grain")) {
    const meta = ensureNestedMap(document, entry, ["config", "meta"]);
    setStringOrDelete(meta, "record_grain", changes.grain);
  }

  if (Object.hasOwn(changes, "transformationStepsText")) {
    const meta = ensureNestedMap(document, entry, ["config", "meta"]);
    const steps = String(changes.transformationStepsText || "")
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);

    if (steps.length) {
      meta.set("transformation_steps", steps);
    } else {
      meta.delete("transformation_steps");
    }
  }

  if (Array.isArray(changes.columns)) {
    updateYamlColumns(document, entry, changes.columns);
  }

  const updatedYaml = document.toString({
    flowCollectionPadding: false,
    lineWidth: 0
  });

  if (updatedYaml === originalYaml) {
    return false;
  }

  await writeFile(yamlPath, updatedYaml, "utf8");
  return true;
}

function updateYamlColumns(document, entry, changedColumns) {
  const columns = ensureSeq(document, entry, "columns");

  for (const changedColumn of changedColumns) {
    if (!changedColumn?.name) {
      continue;
    }

    const column = findOrCreateColumn(document, columns, changedColumn.name);

    if (Object.hasOwn(changedColumn, "description")) {
      column.set("description", String(changedColumn.description || "").trimEnd());
    }

    if (Object.hasOwn(changedColumn, "dataType")) {
      const configMeta = ensureNestedMap(document, column, ["config", "meta"]);
      setStringOrDelete(configMeta, "data_type", changedColumn.dataType);

      const legacyMeta = column.get("meta", true);

      if (isMap(legacyMeta) && legacyMeta.has("data_type")) {
        setStringOrDelete(legacyMeta, "data_type", changedColumn.dataType);
      }
    }

    if (Object.hasOwn(changedColumn, "isPrimaryKey")) {
      const isPrimaryKey = Boolean(changedColumn.isPrimaryKey);
      const configMeta = ensureNestedMap(document, column, ["config", "meta"]);
      const legacyMeta = column.get("meta", true);

      configMeta.set("is_primary_key", isPrimaryKey);

      if (isMap(legacyMeta) && legacyMeta.has("is_primary_key")) {
        legacyMeta.set("is_primary_key", isPrimaryKey);
      }
    }
  }
}

function findOrCreateColumn(document, columns, columnName) {
  const existing = columns.items.find((item) => isMap(item) && item.get("name") === columnName);

  if (existing) {
    return existing;
  }

  const column = document.createNode({
    name: columnName,
    description: "",
    config: {
      meta: {}
    }
  });
  columns.add(column);
  return column;
}

function ensureNestedMap(document, root, keys) {
  return keys.reduce((current, key) => ensureMap(document, current, key), root);
}

function ensureMap(document, parent, key) {
  const current = parent.get(key, true);

  if (isMap(current)) {
    return current;
  }

  const next = document.createNode({});
  parent.set(key, next);
  return parent.get(key, true);
}

function ensureSeq(document, parent, key) {
  const current = parent.get(key, true);

  if (isSeq(current)) {
    return current;
  }

  const next = document.createNode([]);
  parent.set(key, next);
  return parent.get(key, true);
}

function setStringOrDelete(map, key, value) {
  const text = String(value || "").trimEnd();

  if (text) {
    map.set(key, text);
  } else {
    map.delete(key);
  }
}

function hasYamlChanges(changes) {
  return (
    Object.hasOwn(changes, "description") ||
    Object.hasOwn(changes, "grain") ||
    Object.hasOwn(changes, "transformationStepsText") ||
    Array.isArray(changes.columns)
  );
}

function assertEditableSqlPath(filePath) {
  const resolvedPath = assertReadableRepoPath(filePath);

  if (path.extname(resolvedPath) !== ".sql") {
    throw createHttpError(400, "Only dbt model SQL files can be edited from the SQL panel.");
  }

  return resolvedPath;
}

function assertEditableYamlPath(filePath) {
  const resolvedPath = assertReadableRepoPath(filePath);
  const extension = path.extname(resolvedPath);

  if (extension !== ".yml" && extension !== ".yaml") {
    throw createHttpError(400, "Only YAML documentation files can be edited from the DAG Viewer.");
  }

  return resolvedPath;
}

function assertReadableRepoPath(filePath) {
  if (!filePath) {
    throw createHttpError(400, "This node does not have a local file path.");
  }

  const resolvedPath = path.resolve(filePath);
  const relativePath = path.relative(repoRoot, resolvedPath);

  if (relativePath.startsWith("..") || path.isAbsolute(relativePath)) {
    throw createHttpError(400, `Refusing to access a file outside the Tuva checkout: ${filePath}`);
  }

  if (!existsSync(resolvedPath)) {
    throw createHttpError(404, `File does not exist: ${relativePath}`);
  }

  return resolvedPath;
}

async function serveStaticAsset(request, response, requestUrl) {
  const pathname = requestUrl.pathname === "/" ? "/index.html" : decodeURIComponent(requestUrl.pathname);
  const root = pathname.startsWith("/data/") ? distRoot : publicRoot;
  const filePath = path.resolve(root, `.${pathname}`);
  const relativePath = path.relative(root, filePath);

  if (relativePath.startsWith("..") || path.isAbsolute(relativePath)) {
    sendJson(request, response, 403, { error: "Invalid path" });
    return;
  }

  try {
    const fileStat = await stat(filePath);

    if (!fileStat.isFile()) {
      throw new Error("Not a file");
    }

    writeCorsHeaders(request, response);
    response.setHeader("Content-Type", contentTypeForPath(filePath));
    response.setHeader("Cache-Control", "no-store");
    response.writeHead(200);

    if (path.basename(filePath) === "index.html") {
      response.end(await buildDevIndexHtml(request));
      return;
    }

    response.end(await readFile(filePath));
  } catch {
    sendJson(request, response, 404, { error: "Not found" });
  }
}

async function buildDevIndexHtml(request) {
  const indexHtml = await readFile(path.join(publicRoot, "index.html"), "utf8");
  const hostHeader = request.headers.host || `${host}:${port}`;
  const apiBase = `http://${hostHeader}`;
  const devConfig = `window.TUVA_DAG_VIEWER_CONFIG = {
        mode: "live",
        apiBase: "${escapeJsString(apiBase)}",
        dataBaseUrl: "./data"
      };`;

  return indexHtml.replace(/window\.TUVA_DAG_VIEWER_CONFIG = \{[\s\S]*?\n\s*\};/, devConfig);
}

function contentTypeForPath(filePath) {
  const extension = path.extname(filePath).toLowerCase();

  if (extension === ".html") {
    return "text/html; charset=utf-8";
  }

  if (extension === ".js" || extension === ".mjs") {
    return "text/javascript; charset=utf-8";
  }

  if (extension === ".css") {
    return "text/css; charset=utf-8";
  }

  if (extension === ".json") {
    return "application/json; charset=utf-8";
  }

  if (extension === ".svg") {
    return "image/svg+xml";
  }

  if (extension === ".png") {
    return "image/png";
  }

  if (extension === ".ico") {
    return "image/x-icon";
  }

  return "application/octet-stream";
}

function handleEvents(request, response) {
  writeCorsHeaders(request, response);
  response.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-store",
    Connection: "keep-alive"
  });
  response.write(`event: refresh_state\ndata: ${JSON.stringify(refreshState)}\n\n`);
  eventClients.add(response);

  request.on("close", () => {
    eventClients.delete(response);
  });
}

function broadcastEvent(eventName, data) {
  for (const client of eventClients) {
    client.write(`event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`);
  }
}

async function readJsonBody(request) {
  const chunks = [];

  for await (const chunk of request) {
    chunks.push(chunk);
  }

  const text = Buffer.concat(chunks).toString("utf8");

  if (!text.trim()) {
    return {};
  }

  try {
    return JSON.parse(text);
  } catch {
    throw createHttpError(400, "Request body is not valid JSON.");
  }
}

function sendJson(request, response, statusCode, body) {
  writeCorsHeaders(request, response);
  response.writeHead(body?.statusCode || statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  response.end(`${JSON.stringify(body, null, 2)}\n`);
}

function writeCorsHeaders(request, response) {
  const origin = request.headers.origin;

  if (origin && isLoopbackOrigin(origin)) {
    response.setHeader("Access-Control-Allow-Origin", origin);
    response.setHeader("Access-Control-Allow-Credentials", "true");
    response.setHeader("Vary", "Origin");
  }

  response.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  response.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

function isLoopbackRequest(request) {
  const remoteAddress = request.socket.remoteAddress || "";

  return (
    remoteAddress === "127.0.0.1" ||
    remoteAddress === "::1" ||
    remoteAddress === "::ffff:127.0.0.1" ||
    remoteAddress.startsWith("::ffff:127.")
  );
}

function isLoopbackOrigin(origin) {
  try {
    const originUrl = new URL(origin);
    return ["localhost", "127.0.0.1", "::1"].includes(originUrl.hostname);
  } catch {
    return false;
  }
}

function createRefreshState({
  status,
  payloadVersion: nextPayloadVersion = payloadVersion,
  activeTargetKey,
  activeTrigger,
  lastAttemptAt = null,
  lastSuccessAt = null,
  lastError = null
}) {
  return {
    status,
    payloadVersion: nextPayloadVersion,
    activeTargetKey,
    activeTrigger,
    lastAttemptAt,
    lastSuccessAt,
    lastError,
    hasPayload: status !== "failed",
    mode: "local_dev"
  };
}

function normalizeCommandError(error) {
  return {
    message: error instanceof Error ? error.message : String(error),
    detail: error?.output || null,
    command: error?.command || null,
    exitCode: error?.exitCode ?? null,
    occurredAt: new Date().toISOString()
  };
}

function createHttpError(statusCode, message) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

function escapeJsString(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/"/g, "\\\"");
}

main().catch((error) => {
  process.stderr.write(`${error.stack || error}\n`);
  process.exitCode = 1;
});
