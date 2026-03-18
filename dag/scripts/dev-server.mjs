import { spawn } from "node:child_process";
import { createReadStream } from "node:fs";
import { access, readFile, writeFile } from "node:fs/promises";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";

import Papa from "papaparse";
import { parseDocument, Scalar } from "yaml";
import {
  buildLineagePayload,
  DEFAULT_TARGET_KEY,
  getOutputPathForTarget,
  getTargetConfig,
  listTargetConfigs,
  readPersistedLineagePayload,
  persistLineagePayload
} from "./build-appointment-lineage.mjs";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(scriptDir, "..");
const repoRoot = path.resolve(rootDir, "..");
const startingPort = Number(process.env.PORT || 4173);
const dbtLocalScriptPath = process.env.DAG_DBT_LOCAL_PATH || path.join(repoRoot, "scripts", "dbt-local");

const contentTypes = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".ico": "image/x-icon",
  ".jpeg": "image/jpeg",
  ".jpg": "image/jpeg",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml; charset=utf-8"
};

const sseClients = new Set();
const seedDatasetCache = new Map();

const serverState = {
  payload: null,
  refresh: {
    status: "idle",
    payloadVersion: 0,
    activeTargetKey: DEFAULT_TARGET_KEY,
    activeTrigger: "startup",
    lastAttemptAt: null,
    lastSuccessAt: null,
    lastError: null
  },
  activeRefreshPromise: null
};

await hydratePersistedPayload();
const server = await startServer(startingPort);
void startRefreshJob({ trigger: "startup" });

async function createLineageResponse(targetKey = DEFAULT_TARGET_KEY) {
  const payload = await loadPayloadForTarget(targetKey);
  const targets = await listTargetConfigs();

  return {
    payload,
    refresh: createRefreshSnapshot(),
    targets
  };
}

function createRefreshSnapshot() {
  return {
    ...serverState.refresh,
    hasPayload: Boolean(serverState.payload)
  };
}

async function hydratePersistedPayload() {
  try {
    const payload = await readPersistedLineagePayload({ targetKey: DEFAULT_TARGET_KEY });

    serverState.payload = payload;
    serverState.refresh.status = "ready";
    serverState.refresh.payloadVersion = 1;
    serverState.refresh.lastSuccessAt = payload.generatedAt || new Date().toISOString();
  } catch {
    serverState.refresh.status = "idle";
  }
}

async function startRefreshJob({ trigger = "manual", targetKey = DEFAULT_TARGET_KEY } = {}) {
  if (serverState.activeRefreshPromise) {
    return { started: false, refresh: createRefreshSnapshot() };
  }

  await getTargetConfig(targetKey);

  serverState.refresh.status = "refreshing";
  serverState.refresh.activeTargetKey = targetKey;
  serverState.refresh.activeTrigger = trigger;
  serverState.refresh.lastAttemptAt = new Date().toISOString();
  serverState.refresh.lastError = null;

  broadcastRefreshState();

  const refreshPromise = (async () => {
    try {
      process.stdout.write(`[dag] Refresh started (${trigger})\n`);
      await runCommand({
        command: dbtLocalScriptPath,
        args: ["parse"],
        cwd: repoRoot
      });

      const payload = await buildLineagePayload({ targetKey });
      await persistLineagePayload(payload);

      serverState.payload = payload;
      seedDatasetCache.clear();
      serverState.refresh.status = "ready";
      serverState.refresh.activeTargetKey = targetKey;
      serverState.refresh.lastSuccessAt = new Date().toISOString();
      serverState.refresh.lastError = null;
      serverState.refresh.payloadVersion += 1;

      process.stdout.write(
        `[dag] Refresh finished (${trigger}) -> ${getOutputPathForTarget(targetKey)}\n`
      );

      broadcastRefreshState();
      broadcastEvent("lineage_updated", {
        payloadVersion: serverState.refresh.payloadVersion,
        targetKey
      });

      return { ok: true };
    } catch (error) {
      serverState.refresh.status = "failed";
      serverState.refresh.lastError = serializeRefreshError(error);

      process.stderr.write(`[dag] Refresh failed (${trigger})\n`);

      broadcastRefreshState();

      return { ok: false, error };
    } finally {
      serverState.activeRefreshPromise = null;
    }
  })();

  serverState.activeRefreshPromise = refreshPromise;
  return { started: true, refresh: createRefreshSnapshot(), promise: refreshPromise };
}

async function handleRequest(request, response) {
  const requestUrl = new URL(request.url || "/", `http://${request.headers.host}`);
  const pathname = decodeURIComponent(requestUrl.pathname);

  if (pathname === "/api/lineage" && request.method === "GET") {
    const targetKey = requestUrl.searchParams.get("targetKey") || serverState.payload?.target?.key || DEFAULT_TARGET_KEY;
    await getTargetConfig(targetKey);
    writeJson(response, 200, await createLineageResponse(targetKey));
    return;
  }

  if (pathname === "/api/refresh" && request.method === "POST") {
    const body = await readJsonBody(request);
    const targetKey = body?.targetKey || DEFAULT_TARGET_KEY;

    if (serverState.activeRefreshPromise) {
      writeJson(response, 409, {
        error: "A DAG refresh is already running.",
        refresh: createRefreshSnapshot()
      });
      return;
    }

    try {
      const result = await startRefreshJob({ trigger: "manual", targetKey });
      writeJson(response, 202, result);
    } catch (error) {
      writeJson(response, 400, {
        error: error instanceof Error ? error.message : String(error),
        refresh: createRefreshSnapshot()
      });
    }
    return;
  }

  if (pathname === "/api/save-node" && request.method === "POST") {
    await handleSaveNodeRequest(request, response);
    return;
  }

  if (pathname === "/api/events" && request.method === "GET") {
    handleEventsRequest(response);
    return;
  }

  if (pathname === "/api/seed-preview" && request.method === "GET") {
    await handleSeedPreviewRequest(requestUrl, response);
    return;
  }

  await serveStaticRequest(pathname, response);
}

async function handleSaveNodeRequest(request, response) {
  if (serverState.activeRefreshPromise) {
    writeJson(response, 409, {
      error: "A DAG refresh is already running. Save again once the refresh completes.",
      refresh: createRefreshSnapshot()
    });
    return;
  }

  const body = await readJsonBody(request);
  const targetKey = body?.targetKey || DEFAULT_TARGET_KEY;
  const nodeId = body?.nodeId || null;
  const changes = body?.changes || null;

  if (!nodeId) {
    writeJson(response, 400, { error: "Missing required field: nodeId" });
    return;
  }

  if (!changes || typeof changes !== "object") {
    writeJson(response, 400, { error: "Missing required field: changes" });
    return;
  }

  const payload = await loadPayloadForTarget(targetKey);
  const node = payload.nodes.find((candidate) => candidate.id === nodeId) || null;

  if (!node) {
    writeJson(response, 404, { error: `Node not found: ${nodeId}` });
    return;
  }

  if (node.resourceType === "dag") {
    writeJson(response, 400, { error: "Collapsed DAG boundary nodes are not editable yet." });
    return;
  }

  await applyNodeChanges({ node, changes });

  const refreshResult = await startRefreshJob({ trigger: "save", targetKey });

  if (refreshResult.promise) {
    await refreshResult.promise;
  }

  if (serverState.refresh.status === "failed") {
    writeJson(response, 500, {
      error: serverState.refresh.lastError?.message || "Save completed but refresh failed.",
      detail: serverState.refresh.lastError?.detail || null,
      refresh: createRefreshSnapshot()
    });
    return;
  }

  writeJson(response, 200, await createLineageResponse(targetKey));
}

async function applyNodeChanges({ node, changes }) {
  const hasYamlChanges = changes.description !== undefined ||
    changes.grain !== undefined ||
    changes.transformationStepsText !== undefined ||
    (Array.isArray(changes.columns) && changes.columns.length > 0);

  if (hasYamlChanges) {
    await writeNodeYamlChanges({ node, changes });
  }

  if (changes.sql !== undefined) {
    if (!node.paths?.sql) {
      const error = new Error(`No SQL file is available for ${node.name}.`);
      error.statusCode = 400;
      throw error;
    }

    await writeFile(node.paths.sql, normalizeTextBlock(changes.sql), "utf8");
  }
}

async function writeNodeYamlChanges({ node, changes }) {
  if (!node.paths?.yaml || !node.paths?.yamlEntryName) {
    const error = new Error(`No YAML metadata file is available for ${node.name}.`);
    error.statusCode = 400;
    throw error;
  }

  const sourceText = await readFile(node.paths.yaml, "utf8");
  const document = parseDocument(sourceText);
  const collectionKey = node.paths.yamlCollectionKey || (node.resourceType === "seed" ? "seeds" : "models");
  const collection = document.get(collectionKey, true);

  if (!collection?.items) {
    const error = new Error(`Unable to find YAML collection '${collectionKey}' in ${node.paths.yaml}.`);
    error.statusCode = 400;
    throw error;
  }

  const entry = collection.items.find((item) => item?.get?.("name") === node.paths.yamlEntryName);

  if (!entry) {
    const error = new Error(`Unable to find YAML entry '${node.paths.yamlEntryName}' in ${node.paths.yaml}.`);
    error.statusCode = 400;
    throw error;
  }

  if (changes.description !== undefined) {
    setOrDeleteScalar(entry, "description", changes.description, document);
  }

  if (changes.grain !== undefined) {
    setOrDeleteScalar(entry, "grain", changes.grain, document);
  }

  if (changes.transformationStepsText !== undefined) {
    setOrDeleteScalar(entry, "transformation_steps", changes.transformationStepsText, document);
  }

  if (Array.isArray(changes.columns) && changes.columns.length) {
    const columns = ensureSeq(entry, "columns", document);

    for (const columnChange of changes.columns) {
      if (!columnChange?.name) {
        continue;
      }

      let columnEntry = columns.items.find((item) => item?.get?.("name") === columnChange.name);

      if (!columnEntry) {
        columnEntry = document.createNode({ name: columnChange.name });
        columns.items.push(columnEntry);
      }

      if (columnChange.description !== undefined) {
        setOrDeleteScalar(columnEntry, "description", columnChange.description, document);
      }

      if (columnChange.mappingInstructions !== undefined) {
        setOrDeleteScalar(columnEntry, "mapping_instructions", columnChange.mappingInstructions, document);
      }

      if (columnChange.dataType !== undefined) {
        const meta = ensureMapPath(columnEntry, ["config", "meta"], document);
        setOrDeleteScalar(meta, "data_type", columnChange.dataType, document);
      }

      if (columnChange.isPrimaryKey !== undefined) {
        const meta = ensureMapPath(columnEntry, ["config", "meta"], document);

        if (columnChange.isPrimaryKey) {
          meta.set("is_primary_key", true);
        } else {
          meta.delete("is_primary_key");
        }
      }
    }
  }

  await writeFile(node.paths.yaml, String(document), "utf8");
}

function setOrDeleteScalar(parent, key, value, document) {
  const normalized = normalizeOptionalText(value);

  if (!normalized) {
    parent.delete(key);
    return;
  }

  if (document && normalized.includes("\n")) {
    const scalar = document.createNode(normalized);
    scalar.type = Scalar.BLOCK_LITERAL;
    parent.set(key, scalar);
    return;
  }

  parent.set(key, normalized);
}

function ensureSeq(parent, key, document) {
  let sequence = parent.get(key, true);

  if (sequence?.items) {
    return sequence;
  }

  sequence = document.createNode([]);
  parent.set(key, sequence);
  return sequence;
}

function ensureMapPath(parent, pathKeys, document) {
  let current = parent;

  for (const key of pathKeys) {
    let next = current.get(key, true);

    if (!next?.set) {
      next = document.createNode({});
      current.set(key, next);
    }

    current = next;
  }

  return current;
}

function normalizeOptionalText(value) {
  if (value === null || value === undefined) {
    return "";
  }

  const normalized = normalizeTextBlock(String(value)).trim();
  return normalized;
}

function normalizeTextBlock(value) {
  return String(value).replace(/\r\n/g, "\n");
}

async function handleSeedPreviewRequest(requestUrl, response) {
  const targetKey = requestUrl.searchParams.get("targetKey") || serverState.payload?.target?.key || DEFAULT_TARGET_KEY;
  const nodeId = requestUrl.searchParams.get("nodeId");
  const query = (requestUrl.searchParams.get("query") || "").trim();
  const page = clampPositiveInteger(requestUrl.searchParams.get("page"), 1);
  const pageSize = clampPositiveInteger(requestUrl.searchParams.get("pageSize"), 50, 200);

  if (!nodeId) {
    writeJson(response, 400, { error: "Missing required query parameter: nodeId" });
    return;
  }

  const payload = await loadPayloadForTarget(targetKey);
  const node = payload.nodes.find((candidate) => candidate.id === nodeId) || null;

  if (!node || node.resourceType !== "seed") {
    writeJson(response, 404, { error: `Seed node not found for ${nodeId}` });
    return;
  }

  if (!node.seedViewer?.downloadUrl) {
    writeJson(response, 404, { error: `No S3 preview source configured for ${node.name}` });
    return;
  }

  const dataset = await loadSeedDataset(node);
  const filteredRows = query ? dataset.rows.filter((row) => rowMatchesQuery(row, query)) : dataset.rows;
  const startIndex = (page - 1) * pageSize;
  const rows = filteredRows.slice(startIndex, startIndex + pageSize);

  writeJson(response, 200, {
    nodeId: node.id,
    query,
    page,
    pageSize,
    totalRows: dataset.rows.length,
    totalMatches: filteredRows.length,
    headers: dataset.headers,
    rows,
    sourceUrl: node.seedViewer.downloadUrl,
    sourceFileName: node.seedViewer.fileName,
    family: node.seedViewer.family
  });
}

async function serveStaticRequest(pathname, response) {
  let normalizedPath = pathname;
  let filePathRoot = rootDir;

  if (normalizedPath === "/") {
    normalizedPath = "/index.html";
  }

  if (normalizedPath.startsWith("/docs-static/")) {
    normalizedPath = normalizedPath.replace(/^\/docs-static\//, "/");
    filePathRoot = path.join(repoRoot, "docs", "static");
  }

  const filePath = path.join(filePathRoot, normalizedPath);

  if (!filePath.startsWith(filePathRoot)) {
    response.writeHead(403, { "Content-Type": "text/plain; charset=utf-8" });
    response.end("Forbidden");
    return;
  }

  try {
    await access(filePath);
  } catch (error) {
    error.statusCode = 404;
    throw error;
  }

  const contentType = contentTypes[path.extname(filePath)] || "application/octet-stream";
  response.writeHead(200, {
    "Cache-Control": "no-store",
    "Content-Type": contentType
  });
  createReadStream(filePath).pipe(response);
}

function handleEventsRequest(response) {
  response.writeHead(200, {
    "Cache-Control": "no-store",
    Connection: "keep-alive",
    "Content-Type": "text/event-stream; charset=utf-8"
  });

  response.write("retry: 2000\n\n");
  sseClients.add(response);

  sendEvent(response, "refresh_state", createRefreshSnapshot());

  const keepAlive = setInterval(() => {
    response.write(":keep-alive\n\n");
  }, 15000);

  response.on("close", () => {
    clearInterval(keepAlive);
    sseClients.delete(response);
  });
}

function broadcastRefreshState() {
  broadcastEvent("refresh_state", createRefreshSnapshot());
}

async function loadPayloadForTarget(targetKey = DEFAULT_TARGET_KEY) {
  return serverState.payload?.target?.key === targetKey ? serverState.payload : buildLineagePayload({ targetKey });
}

async function loadSeedDataset(node) {
  const cacheKey = node.seedViewer?.downloadUrl;

  if (!cacheKey) {
    throw new Error(`Seed preview source missing for ${node.name}`);
  }

  const cached = seedDatasetCache.get(cacheKey);

  if (cached) {
    return cached instanceof Promise ? cached : Promise.resolve(cached);
  }

  const promise = (async () => {
    const response = await fetch(cacheKey, {
      headers: {
        Accept: "application/gzip,application/x-gzip,application/octet-stream,text/csv"
      }
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch seed preview: ${response.status} ${response.statusText}`);
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    const csvText = decodeSeedBuffer(buffer);
    const parsed = Papa.parse(csvText, {
      header: false,
      dynamicTyping: false,
      skipEmptyLines: true
    });

    if (Array.isArray(parsed.errors) && parsed.errors.some((error) => error.code !== "UndetectableDelimiter")) {
      const [firstError] = parsed.errors;
      throw new Error(`Unable to parse seed preview: ${firstError.message}`);
    }

    const rows = Array.isArray(parsed.data) ? parsed.data.filter(Array.isArray) : [];
    const columnCount = rows[0]?.length || node.columns?.length || 0;

    return {
      headers: buildSeedHeaders(node, columnCount),
      rows
    };
  })();

  seedDatasetCache.set(cacheKey, promise);

  try {
    const dataset = await promise;
    seedDatasetCache.set(cacheKey, dataset);
    return dataset;
  } catch (error) {
    seedDatasetCache.delete(cacheKey);
    throw error;
  }
}

function decodeSeedBuffer(buffer) {
  try {
    return gunzipSync(buffer).toString("utf8");
  } catch {
    return buffer.toString("utf8");
  }
}

function buildSeedHeaders(node, columnCount) {
  const documentedHeaders = Array.isArray(node.columns) ? node.columns.map((column) => column.name).filter(Boolean) : [];

  if (!documentedHeaders.length) {
    return Array.from({ length: columnCount }, (_, index) => `Column ${index + 1}`);
  }

  if (documentedHeaders.length >= columnCount) {
    return documentedHeaders.slice(0, columnCount);
  }

  const fallbackHeaders = Array.from({ length: columnCount - documentedHeaders.length }, (_, index) => {
    return `Column ${documentedHeaders.length + index + 1}`;
  });

  return [...documentedHeaders, ...fallbackHeaders];
}

function rowMatchesQuery(row, query) {
  const normalizedQuery = query.toLowerCase();

  return row.some((value) => String(value || "").toLowerCase().includes(normalizedQuery));
}

function clampPositiveInteger(value, fallback, max = 1000) {
  const parsed = Number.parseInt(String(value || ""), 10);

  if (!Number.isFinite(parsed) || parsed < 1) {
    return fallback;
  }

  return Math.min(parsed, max);
}

function broadcastEvent(name, payload) {
  for (const client of sseClients) {
    sendEvent(client, name, payload);
  }
}

function sendEvent(response, name, payload) {
  response.write(`event: ${name}\n`);
  response.write(`data: ${JSON.stringify(payload)}\n\n`);
}

async function startServer(port) {
  const maxAttempts = 15;

  for (let offset = 0; offset < maxAttempts; offset += 1) {
    const candidatePort = port + offset;
    const candidateServer = http.createServer((request, response) => {
      handleRequest(request, response).catch((error) => {
        const statusCode = error?.statusCode || 500;
        const message = error instanceof Error ? error.message : "Unexpected server error";
        writeJson(response, statusCode, {
          error: message,
          refresh: createRefreshSnapshot()
        });
      });
    });

    try {
      await new Promise((resolve, reject) => {
        candidateServer.once("error", reject);
        candidateServer.listen(candidatePort, "0.0.0.0", resolve);
      });

      process.stdout.write(`Serving ${rootDir} at http://localhost:${candidatePort}\n`);
      return candidateServer;
    } catch (error) {
      candidateServer.close();

      if (error?.code !== "EADDRINUSE") {
        throw error;
      }
    }
  }

  throw new Error(`No open port found between ${port} and ${port + maxAttempts - 1}.`);
}

function writeJson(response, statusCode, payload) {
  response.writeHead(statusCode, {
    "Cache-Control": "no-store",
    "Content-Type": "application/json; charset=utf-8"
  });
  response.end(`${JSON.stringify(payload)}\n`);
}

async function readJsonBody(request) {
  const chunks = [];

  for await (const chunk of request) {
    chunks.push(chunk);
  }

  if (!chunks.length) {
    return null;
  }

  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    const error = new Error("Request body must be valid JSON.");
    error.statusCode = 400;
    throw error;
  }
}

function serializeRefreshError(error) {
  return {
    message: error instanceof Error ? error.message : String(error),
    command: error?.command || null,
    detail: error?.detail || null,
    exitCode: error?.exitCode ?? null,
    occurredAt: new Date().toISOString()
  };
}

async function runCommand({ command, args, cwd }) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"]
    });

    let combinedOutput = "";

    child.stdout.on("data", (chunk) => {
      combinedOutput = appendOutput(combinedOutput, chunk.toString("utf8"));
    });

    child.stderr.on("data", (chunk) => {
      combinedOutput = appendOutput(combinedOutput, chunk.toString("utf8"));
    });

    child.on("error", (error) => {
      reject(
        createCommandError({
          message: error.message,
          command: formatCommand(command, args),
          exitCode: null,
          detail: stripAnsi(combinedOutput)
        })
      );
    });

    child.on("close", (exitCode) => {
      if (exitCode === 0) {
        resolve({
          command: formatCommand(command, args),
          detail: stripAnsi(combinedOutput)
        });
        return;
      }

      reject(
        createCommandError({
          message: `Command failed with exit code ${exitCode}: ${formatCommand(command, args)}`,
          command: formatCommand(command, args),
          exitCode,
          detail: stripAnsi(combinedOutput)
        })
      );
    });
  });
}

function appendOutput(existing, nextChunk) {
  const combined = `${existing}${nextChunk}`;
  const maxChars = 60000;

  if (combined.length <= maxChars) {
    return combined;
  }

  return combined.slice(-maxChars);
}

function stripAnsi(text) {
  return text.replace(/\u001B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])/g, "").trim();
}

function formatCommand(command, args) {
  return [path.relative(repoRoot, command) || command, ...args].join(" ");
}

function createCommandError({ message, command, exitCode, detail }) {
  const error = new Error(message);

  error.command = command;
  error.exitCode = exitCode;
  error.detail = detail;

  return error;
}

server.on("close", () => {
  for (const client of sseClients) {
    client.end();
  }
});
