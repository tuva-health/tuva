const initialSearchParams = new URLSearchParams(window.location.search);
const runtimeConfig = window.TUVA_DAG_VIEWER_CONFIG || {};
const VIEWER_MODE = runtimeConfig.mode || initialSearchParams.get("mode") || "live";
const IS_STATIC_MODE = VIEWER_MODE === "static";
const API_BASE_URL = runtimeConfig.apiBase || initialSearchParams.get("apiBase") || "http://127.0.0.1:8000";
const STATIC_DATA_BASE_URL = runtimeConfig.dataBaseUrl || initialSearchParams.get("dataBase") || "./data";
const LINEAGE_API_URL = `${API_BASE_URL}/api/dag/lineage`;
const REFRESH_API_URL = `${API_BASE_URL}/api/dag/refresh`;
const EVENTS_API_URL = `${API_BASE_URL}/api/dag/events`;
const SEED_PREVIEW_API_URL = `${API_BASE_URL}/api/dag/seed-preview`;
const SAVE_NODE_API_URL = `${API_BASE_URL}/api/dag/save-node`;

const NODE_WIDTH = 290;
const NODE_HEIGHT = 96;
const DEFAULT_SCENE_SIZE = Object.freeze({
  width: 1560,
  height: 1180
});
const MIN_COLUMN_GAP = 340;
const MAX_COLUMN_GAP = 440;
const MIN_MODEL_VERTICAL_GAP = 122;
const MAX_MODEL_VERTICAL_GAP = 176;
const MIN_SEED_VERTICAL_GAP = 110;
const MAX_SEED_VERTICAL_GAP = 144;
const SEED_HORIZONTAL_GAP = 36;
const SCENE_MARGIN_LEFT = 180;
const SCENE_MARGIN_RIGHT = 240;
const SCENE_MARGIN_TOP = 84;
const SCENE_MARGIN_BOTTOM = 140;
const SEED_BAND_PADDING = 48;
const MIN_BAND_GAP = 116;
const MAX_BAND_GAP = 180;
const DRAG_THRESHOLD = 6;
const MIN_SCALE = 0.18;
const MAX_SCALE = 1.8;
const FIT_SIDE_INSET = 36;
const FIT_VERTICAL_BUFFER = 28;
const LAYOUT_STORAGE_VERSION = 3;
const DEFAULT_VIEWPORT = Object.freeze({
  x: 160,
  y: 120,
  scale: 1
});
const SEED_PREVIEW_PAGE_SIZE = 50;
const INITIAL_LAUNCHER_OPEN = initialSearchParams.get("launcher") === "open";
const INITIAL_OPEN_NODE_ID = initialSearchParams.get("openNode");
const INITIAL_EDIT_FIELD = initialSearchParams.get("edit");
const INITIAL_MODAL_TAB = initialSearchParams.get("tab");
const SYSTEM_OVERVIEW_TARGET_KEY = "system_overview";
const SYSTEM_OVERVIEW_LAYOUT_VERSION = "9";
const SYSTEM_OVERVIEW_ENTRY_WIDTH = 212;
const SYSTEM_OVERVIEW_ENTRY_HEIGHT = 52;
const SYSTEM_OVERVIEW_STAGE_PADDING_X = 24;
const SYSTEM_OVERVIEW_STAGE_PADDING_Y = 18;
const SYSTEM_OVERVIEW_NORMALIZATION_STAGE_PADDING_Y = 10;
const SYSTEM_OVERVIEW_HIDDEN_POSITION = Object.freeze({
  x: 2240,
  y: 2060
});

const SYSTEM_OVERVIEW_STAGE_TITLES = Object.freeze({
  inputLayer: "Input Layer",
  claimsNormalization: "Claims Normalization",
  claimsPreprocessing: "Claims Preprocessing",
  coreDataModel: "Core Data Model",
  dataMarts: "Data Marts"
});

const SYSTEM_OVERVIEW_ENTRY_LAYOUT = Object.freeze([
  { id: "input-medical-claim", targetKey: "input_layer__medical_claim", label: "medical_claim", stage: "inputLayer", group: "inputClaims", x: 138, y: 214 },
  { id: "input-pharmacy-claim", targetKey: "input_layer__pharmacy_claim", label: "pharmacy_claim", stage: "inputLayer", group: "inputClaims", x: 138, y: 290 },
  { id: "input-eligibility", targetKey: "input_layer__eligibility", label: "eligibility", stage: "inputLayer", group: "inputClaims", x: 138, y: 366 },

  { id: "input-provider-attribution", targetKey: "input_layer__provider_attribution", label: "provider_attribution", stage: "inputLayer", group: "inputNonClaims", x: 138, y: 474 },
  { id: "input-condition", targetKey: "input_layer__condition", label: "condition", stage: "inputLayer", group: "inputNonClaims", x: 138, y: 582 },
  { id: "input-patient", targetKey: "input_layer__patient", label: "patient", stage: "inputLayer", group: "inputNonClaims", x: 138, y: 658 },
  { id: "input-practitioner", targetKey: "input_layer__practitioner", label: "practitioner", stage: "inputLayer", group: "inputNonClaims", x: 138, y: 734 },
  { id: "input-procedure", targetKey: "input_layer__procedure", label: "procedure", stage: "inputLayer", group: "inputNonClaims", x: 138, y: 810 },
  { id: "input-location", targetKey: "input_layer__location", label: "location", stage: "inputLayer", group: "inputNonClaims", x: 138, y: 886 },
  { id: "input-encounter", targetKey: "input_layer__encounter", label: "encounter", stage: "inputLayer", group: "inputNonClaims", x: 138, y: 962 },
  { id: "input-lab-result", targetKey: "input_layer__lab_result", label: "lab_result", stage: "inputLayer", group: "inputNonClaims", x: 138, y: 1038 },
  { id: "input-medication", targetKey: "input_layer__medication", label: "medication", stage: "inputLayer", group: "inputNonClaims", x: 138, y: 1114 },
  { id: "input-immunization", targetKey: "input_layer__immunization", label: "immunization", stage: "inputLayer", group: "inputNonClaims", x: 138, y: 1222 },
  { id: "input-observation", targetKey: "input_layer__observation", label: "observation", stage: "inputLayer", group: "inputNonClaims", x: 138, y: 1298 },
  { id: "input-appointment", targetKey: "input_layer__appointment", label: "appointment", stage: "inputLayer", group: "inputNonClaims", x: 138, y: 1374 },

  { id: "overview-normalized-medical-claim", targetKey: "claims_normalization__medical_claim", label: "medical_claim", stage: "claimsNormalization", group: "claimsNormalization", x: 536, y: 454 },
  { id: "overview-normalized-pharmacy-claim", targetKey: "claims_normalization__pharmacy_claim", label: "pharmacy_claim", stage: "claimsNormalization", group: "claimsNormalization", x: 536, y: 530 },
  { id: "overview-normalized-eligibility", targetKey: "claims_normalization__eligibility", label: "eligibility", stage: "claimsNormalization", group: "claimsNormalization", x: 536, y: 606 },

  { id: "claims-service-categories", targetKey: "service_categories", label: "service_categories", stage: "claimsPreprocessing", group: "claimsPreprocessing", x: 874, y: 214 },
  { id: "claims-encounters", targetKey: "encounters", label: "encounters", stage: "claimsPreprocessing", group: "claimsPreprocessing", x: 874, y: 290 },
  { id: "claims-claims-enrollment", targetKey: "claims_enrollment", label: "claims_enrollment", stage: "claimsPreprocessing", group: "claimsPreprocessing", x: 874, y: 366 },

  { id: "core-medical-claim", targetKey: "medical_claim", label: "medical_claim", stage: "coreDataModel", group: "coreClaims", x: 1318, y: 214 },
  { id: "core-pharmacy-claim", targetKey: "pharmacy_claim", label: "pharmacy_claim", stage: "coreDataModel", group: "coreClaims", x: 1318, y: 290 },
  { id: "core-eligibility", targetKey: "eligibility", label: "eligibility", stage: "coreDataModel", group: "coreClaims", x: 1318, y: 366 },
  { id: "core-member-months", targetKey: "member_months", label: "member_months", stage: "coreDataModel", group: "coreClaims", x: 1318, y: 442 },

  { id: "core-condition", targetKey: "condition", label: "condition", stage: "coreDataModel", group: "coreNonClaims", x: 1318, y: 582 },
  { id: "core-patient", targetKey: "patient", label: "patient", stage: "coreDataModel", group: "coreNonClaims", x: 1318, y: 658 },
  { id: "core-practitioner", targetKey: "practitioner", label: "practitioner", stage: "coreDataModel", group: "coreNonClaims", x: 1318, y: 734 },
  { id: "core-procedure", targetKey: "procedure", label: "procedure", stage: "coreDataModel", group: "coreNonClaims", x: 1318, y: 810 },
  { id: "core-location", targetKey: "location", label: "location", stage: "coreDataModel", group: "coreNonClaims", x: 1318, y: 886 },
  { id: "core-encounter", targetKey: "encounter", label: "encounter", stage: "coreDataModel", group: "coreNonClaims", x: 1318, y: 962 },
  { id: "core-lab-result", targetKey: "lab_result", label: "lab_result", stage: "coreDataModel", group: "coreNonClaims", x: 1318, y: 1038 },
  { id: "core-medication", targetKey: "medication", label: "medication", stage: "coreDataModel", group: "coreNonClaims", x: 1318, y: 1114 },
  { id: "core-person-id-crosswalk", targetKey: "person_id_crosswalk", label: "person_id_crosswalk", stage: "coreDataModel", group: "coreNonClaims", x: 1318, y: 1190 },
  { id: "core-immunization", targetKey: "immunization", label: "immunization", stage: "coreDataModel", group: "coreNonClaims", x: 1318, y: 1298 },
  { id: "core-observation", targetKey: "observation", label: "observation", stage: "coreDataModel", group: "coreNonClaims", x: 1318, y: 1374 },
  { id: "core-appointment", targetKey: "appointment", label: "appointment", stage: "coreDataModel", group: "coreNonClaims", x: 1318, y: 1450 },

  { id: "mart-ahrq-measures", targetKey: "ahrq_measures", label: "ahrq_measures", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 214 },
  { id: "mart-ed-classification", targetKey: "ed_classification", label: "ed_classification", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 290 },
  { id: "mart-financial-pmpm", targetKey: "financial_pmpm", label: "financial_pmpm", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 366 },
  { id: "mart-pharmacy", targetKey: "pharmacy", label: "pharmacy", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 442 },
  { id: "mart-readmissions", targetKey: "readmissions", label: "readmissions", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 518 },

  { id: "mart-ccsr", targetKey: "ccsr", label: "ccsr", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 658 },
  { id: "mart-chronic-conditions", targetKey: "chronic_conditions", label: "chronic_conditions", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 734 },
  { id: "mart-provider-attribution", targetKey: "provider_attribution", label: "provider_attribution", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 810 },
  { id: "mart-quality-measures", targetKey: "quality_measures", label: "quality_measures", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 886 },
  { id: "mart-semantic-layer", targetKey: "semantic_layer", label: "semantic_layer", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 962 },

  { id: "mart-clinical-concept-library", targetKey: "clinical_concept_library", label: "clinical_concept_library", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 1102 },
  { id: "mart-cms-hcc", targetKey: "cms_hcc", label: "cms_hcc", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 1178 },
  { id: "mart-hcc-recapture", targetKey: "hcc_recapture", label: "hcc_recapture", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 1254 },
  { id: "mart-hcc-suspecting", targetKey: "hcc_suspecting", label: "hcc_suspecting", stage: "dataMarts", group: "dataMartsAll", x: 1740, y: 1330 }
]);

const SYSTEM_OVERVIEW_CONNECTOR_SPECS = Object.freeze([
  {
    from: { type: "stage", key: "inputLayer", side: "right", alignEntryId: "input-eligibility" },
    to: { type: "stage", key: "claimsNormalization", side: "left", alignEntryId: "overview-normalized-pharmacy-claim" }
  },
  {
    from: { type: "stage", key: "inputLayer", side: "right", alignEntryId: "input-patient" },
    to: { type: "stage", key: "coreDataModel", side: "left", alignEntryId: "core-patient" }
  },
  {
    from: { type: "stage", key: "claimsNormalization", side: "right", alignEntryId: "overview-normalized-pharmacy-claim" },
    to: { type: "stage", key: "claimsPreprocessing", side: "left", alignEntryId: "claims-encounters" }
  },
  {
    from: { type: "stage", key: "claimsNormalization", side: "right", alignEntryId: "overview-normalized-pharmacy-claim" },
    to: { type: "stage", key: "coreDataModel", side: "left", alignEntryId: "core-eligibility" }
  },
  {
    from: { type: "stage", key: "claimsPreprocessing", side: "right", alignEntryId: "claims-encounters" },
    to: { type: "stage", key: "coreDataModel", side: "left", alignEntryId: "core-patient" }
  },
  {
    from: { type: "stage", key: "coreDataModel", side: "right", alignEntryId: "core-patient" },
    to: { type: "stage", key: "dataMarts", side: "left", alignEntryId: "mart-ccsr" }
  }
]);

const nodeTypeColors = {
  input: "#d86b63",
  intermediate: "#b5b2ab",
  output: "#66b1e2",
  terminology: "#ffcc08"
};

const state = {
  payload: null,
  capabilities: {
    canEdit: false
  },
  targets: [],
  refresh: createDefaultRefreshState(),
  launcherOpen: INITIAL_LAUNCHER_OPEN,
  activeNodeId: null,
  openNodeId: null,
  modalTabNodeId: null,
  modalTab: null,
  editor: createEmptyEditorState(),
  eventsConnected: false,
  seedPreviewByNodeId: {},
  positions: {},
  sceneSize: { ...DEFAULT_SCENE_SIZE },
  viewport: { ...DEFAULT_VIEWPORT },
  layoutMeta: null,
  drag: null,
  needsInitialFit: false
};

const app = document.querySelector("#app");
const persistViewportDebounced = debounce(storeViewport, 160);
const persistPositionsDebounced = debounce(storePositions, 160);
let sceneSyncFrame = null;
let viewportSyncFrame = null;
const requestSeedPreviewDebounced = debounce((nodeId, query) => {
  requestSeedPreview(nodeId, { query, page: 1 });
}, 220);

boot();

async function boot() {
  try {
    await fetchLineage({ preserveSelection: false, targetKey: getInitialTargetKey() });
    if (!IS_STATIC_MODE) {
      connectToEvents();
    }
    window.addEventListener("resize", handleResize);
    window.addEventListener("keydown", handleGlobalKeydown);
  } catch (error) {
    renderFatalError(error);
  }
}

async function fetchLineage({ preserveSelection = true, targetKey = null } = {}) {
  const requestedTargetKey = targetKey || state.payload?.target?.key || state.refresh.activeTargetKey || "appointment";
  let response;

  if (IS_STATIC_MODE) {
    const requestUrl = `${STATIC_DATA_BASE_URL}/${encodeURIComponent(requestedTargetKey)}-lineage.json`;
    response = await fetch(requestUrl, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Failed to load ${requestUrl}: ${response.status}`);
    }
  } else {
    const requestUrl = `${LINEAGE_API_URL}?targetKey=${encodeURIComponent(requestedTargetKey)}`;
    response = await fetch(requestUrl, { cache: "no-store", credentials: "include" });
    if (!response.ok) {
      throw new Error(`Failed to load ${requestUrl}: ${response.status}`);
    }
  }

  const body = await response.json();
  applyLineageResponse(body, { preserveSelection });
}

function applyLineageResponse(body, { preserveSelection = true } = {}) {
  const previousPayload = state.payload;
  const previousTargetKey = previousPayload?.target?.key || null;
  const previousActiveNodeId = state.activeNodeId;
  const previousOpenNodeId = state.openNodeId;
  const previousViewport = { ...state.viewport };
  const previousPositions = { ...state.positions };

  state.payload = body.payload || null;
  state.capabilities = body.capabilities || { canEdit: false };
  state.targets = Array.isArray(body.targets) ? body.targets : state.targets;
  state.refresh = body.refresh || createDefaultRefreshState();
  state.activeNodeId = chooseNodeId({
    payload: state.payload,
    preferredNodeId: preserveSelection ? previousActiveNodeId : null,
    allowFallback: false
  });
  state.openNodeId = chooseOpenNodeId({
    payload: state.payload,
    preferredNodeId: preserveSelection ? previousOpenNodeId : INITIAL_OPEN_NODE_ID
  });
  syncEditorStateWithOpenNode();

  initializeSceneState({
    previousTargetKey,
    previousViewport,
    previousPositions,
    preserveSelection
  });

  render();
}

function initializeSceneState({ previousTargetKey, previousViewport, previousPositions, preserveSelection }) {
  if (!state.payload) {
    state.positions = {};
    state.sceneSize = { ...DEFAULT_SCENE_SIZE };
    state.viewport = { ...DEFAULT_VIEWPORT };
    state.layoutMeta = null;
    state.needsInitialFit = false;
    return;
  }

  const targetKey = state.payload.target?.key || "appointment";
  const previousLayoutMeta = previousTargetKey === targetKey ? state.layoutMeta : null;
  const storedPositions = readStorageJson(getPositionsStorageKey(targetKey));
  const storedViewport = readStorageJson(getViewportStorageKey(targetKey));
  const storedLayoutMeta = readLayoutMeta(targetKey);
  const layoutPlan = deriveLayoutPlan(state.payload.nodes, state.payload.edges);
  const sameTarget = previousTargetKey === targetKey;
  const hasMatchingStoredLayout = isMatchingLayoutMeta(storedLayoutMeta, layoutPlan.signature);
  const allowMismatchedStoredManualLayout = targetKey !== SYSTEM_OVERVIEW_TARGET_KEY;
  const shouldPreserveCurrentPositions = Boolean(
    sameTarget &&
      preserveSelection &&
      isCurrentLayoutMeta(previousLayoutMeta) &&
      previousLayoutMeta.manualAdjusted
  );
  const shouldReuseStoredPositions = Boolean(
    !shouldPreserveCurrentPositions &&
      isCurrentLayoutMeta(storedLayoutMeta) &&
      storedLayoutMeta.manualAdjusted &&
      (allowMismatchedStoredManualLayout || hasMatchingStoredLayout)
  );

  state.sceneSize = layoutPlan.sceneSize;
  const nextPositions = buildNodePositions(state.payload.nodes, {
    currentPositions: shouldPreserveCurrentPositions ? previousPositions : null,
    storedPositions: shouldReuseStoredPositions || hasMatchingStoredLayout ? storedPositions : null,
    defaultPositions: layoutPlan.defaultPositions,
    sceneSize: state.sceneSize
  });

  state.positions = state.payload.target?.key === SYSTEM_OVERVIEW_TARGET_KEY
    ? {
        ...nextPositions,
        ...buildSystemOverviewEntryPositions({
          currentPositions: shouldPreserveCurrentPositions ? previousPositions : null,
          storedPositions: shouldReuseStoredPositions || hasMatchingStoredLayout ? storedPositions : null,
          defaultPositions: layoutPlan.defaultPositions,
          sceneSize: state.sceneSize
        })
      }
    : nextPositions;
  state.layoutMeta = createLayoutMeta(layoutPlan, {
    manualAdjusted:
      shouldPreserveCurrentPositions ||
      Boolean(isCurrentLayoutMeta(storedLayoutMeta) && storedLayoutMeta.manualAdjusted),
    viewportAdjusted:
      Boolean(
        sameTarget &&
          preserveSelection &&
          isCurrentLayoutMeta(previousLayoutMeta) &&
          previousLayoutMeta.signature === layoutPlan.signature &&
          previousLayoutMeta.viewportAdjusted
      ) ||
      Boolean(hasMatchingStoredLayout && storedLayoutMeta.viewportAdjusted)
  });

  if (
    sameTarget &&
    preserveSelection &&
    isCurrentLayoutMeta(previousLayoutMeta) &&
    previousLayoutMeta.signature === layoutPlan.signature &&
    (previousLayoutMeta.manualAdjusted || previousLayoutMeta.viewportAdjusted)
  ) {
    state.viewport = clampViewport(previousViewport);
    state.needsInitialFit = false;
    return;
  }

  if (hasMatchingStoredLayout && isViewportLike(storedViewport)) {
    state.viewport = clampViewport(storedViewport);
    state.needsInitialFit = false;
    return;
  }

  state.viewport = { ...DEFAULT_VIEWPORT };
  state.needsInitialFit = true;
}

function createEmptyEditorState() {
  return {
    nodeId: null,
    isEditing: false,
    isSaving: false,
    dirty: false,
    focusField: null,
    error: null,
    initialDraft: null,
    draft: null
  };
}

function connectToEvents() {
  const eventSource = new EventSource(EVENTS_API_URL, { withCredentials: true });

  eventSource.addEventListener("open", () => {
    state.eventsConnected = true;
    syncStatusOnly();
  });

  eventSource.addEventListener("error", () => {
    state.eventsConnected = false;
    syncStatusOnly();
  });

  eventSource.addEventListener("refresh_state", (event) => {
    state.refresh = JSON.parse(event.data);
    render();
  });

  eventSource.addEventListener("lineage_updated", async () => {
    await fetchLineage({
      preserveSelection: true,
      targetKey: state.payload?.target?.key || state.refresh.activeTargetKey || "appointment"
    });
  });
}

async function handleRefreshClick() {
  if (IS_STATIC_MODE) {
    return;
  }

  try {
    const response = await fetch(REFRESH_API_URL, {
      method: "POST",
      credentials: "include",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        targetKey: state.payload?.target?.key || "appointment"
      })
    });

    const body = await response.json();

    if (!response.ok && response.status !== 409) {
      throw new Error(body.error || `Refresh failed with status ${response.status}`);
    }

    if (body.refresh) {
      state.refresh = body.refresh;
      render();
    }
  } catch (error) {
    state.refresh = {
      ...state.refresh,
      status: "failed",
      lastError: {
        message: error instanceof Error ? error.message : String(error),
        detail: null,
        command: null,
        exitCode: null,
        occurredAt: new Date().toISOString()
      }
    };
    render();
  }
}

function render() {
  if (!state.payload) {
    renderStatusPage();
    return;
  }

  const payload = state.payload;
  syncEditorStateWithOpenNode();
  const openNode = getOpenNode();
  const sceneWidth = state.sceneSize.width;
  const sceneHeight = state.sceneSize.height;

  app.innerHTML = `
    <div class="app-shell">
      ${state.launcherOpen ? '<button class="launcher-backdrop" id="launcher-backdrop" type="button" aria-label="Close DAG picker"></button>' : ""}
      <header class="top-controls app-header">
        <a class="site-brand" href="https://www.thetuvaproject.com" target="_blank" rel="noreferrer" aria-label="The Tuva Project docs">
          <img class="site-brand-logo" src="./assets/the_tuva_project_logo_new.png" alt="The Tuva Project" />
        </a>
        <div class="app-header-title">DAG Viewer</div>
        ${renderDagLauncher()}
        <div class="app-header-actions">
          ${renderHeaderActions()}
        </div>
      </header>
      ${renderRefreshErrorNotice()}

      <div class="zoom-controls">
        <button class="zoom-button zoom-button-wide" id="fit-button" type="button">Resize</button>
        <button class="zoom-button" id="zoom-out-button" type="button" aria-label="Zoom out">-</button>
        <div class="zoom-value" id="zoom-value">${Math.round(state.viewport.scale * 100)}%</div>
        <button class="zoom-button" id="zoom-in-button" type="button" aria-label="Zoom in">+</button>
      </div>

      <main class="canvas-stage" id="canvas-stage" aria-label="Lineage canvas">
        <div
          class="canvas-scene"
          id="canvas-scene"
          style="width:${sceneWidth}px;height:${sceneHeight}px;"
        >
          ${
            isSystemOverviewActive()
              ? renderSystemOverviewScene(payload, sceneWidth, sceneHeight)
              : `
                <svg
                  class="edges-layer"
                  id="edges-layer"
                  viewBox="0 0 ${sceneWidth} ${sceneHeight}"
                  width="${sceneWidth}"
                  height="${sceneHeight}"
                  aria-hidden="true"
                ></svg>
                ${payload.nodes.map(renderNode).join("")}
              `
          }
        </div>
      </main>

      ${openNode ? renderModal(openNode) : ""}
    </div>
  `;

  bindHudEvents();
  bindCanvasEvents();
  bindModalEvents();
  bindSeedPreviewEvents(openNode);
  primeOpenNodeData(openNode);

  requestAnimationFrame(() => {
    if (state.needsInitialFit) {
      fitViewportToNodes({ persist: true });
    } else {
      syncScene();
    }
  });
}

function renderNode(node) {
  const position = state.positions[node.id] || { x: 120, y: 120 };
  const accentColor = resolveNodeColor(node.nodeType);
  const isBoundary = isDagBoundaryNode(node);
  const primaryLabel = isBoundary ? node.name : node.technical?.alias || node.name;
  const secondaryLabel = isBoundary
    ? node.folderLabel || node.layer || ""
    : node.technical?.schemaName || node.folderLabel || node.layer || "";

  return `
    <button
      class="dag-node ${node.id === state.activeNodeId ? "is-active" : ""} ${node.id === state.openNodeId ? "is-open" : ""} ${
        isBoundary ? "is-dag-boundary" : ""
      }"
      data-node-id="${escapeAttribute(node.id)}"
      type="button"
      style="left:${position.x}px;top:${position.y}px;--node-color:${accentColor};"
    >
      <span class="dag-node-accent" aria-hidden="true"></span>
      <span class="dag-node-copy">
        <span class="dag-node-header">
          <span class="dag-node-name">${escapeHtml(primaryLabel)}</span>
          ${isBoundary ? '<span class="dag-node-badge">DAG</span>' : ""}
        </span>
        ${secondaryLabel ? `<span class="dag-node-layer">${escapeHtml(secondaryLabel)}</span>` : ""}
      </span>
    </button>
  `;
}

function renderSystemOverviewScene(payload, sceneWidth, sceneHeight) {
  const model = buildSystemOverviewVisualModel(payload.nodes);

  return `
    <svg
      class="system-overview-connectors"
      viewBox="0 0 ${sceneWidth} ${sceneHeight}"
      width="${sceneWidth}"
      height="${sceneHeight}"
      aria-hidden="true"
    >
      <defs>
        <marker id="overview-arrowhead" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto">
          <path d="M 0 0 L 10 5 L 0 10 z" fill="#3a3936"></path>
        </marker>
      </defs>
      ${model.connectors.map((path) => `<path class="system-overview-connector" d="${path}"></path>`).join("")}
    </svg>
    ${model.stages
      .map(
        (stage) => `
          <div
            class="system-overview-stage-frame stage-${escapeAttribute(stage.key)}"
            data-stage-key="${escapeAttribute(stage.key)}"
            style="left:${stage.bounds.minX}px;top:${stage.bounds.minY}px;width:${stage.bounds.width}px;height:${stage.bounds.height}px;"
          ></div>
          <div
            class="system-overview-stage-label"
            data-stage-key="${escapeAttribute(stage.key)}"
            style="left:${stage.labelX}px;top:${stage.labelY}px;"
          >${escapeHtml(stage.title)}</div>
        `
      )
      .join("")}
    ${model.entries.map(renderSystemOverviewEntry).join("")}
  `;
}

function renderSystemOverviewEntry(entry) {
  return `
    <button
      class="system-overview-entry stage-${escapeAttribute(entry.stage)}"
      data-entry-id="${escapeAttribute(entry.id)}"
      data-target-key="${escapeAttribute(entry.targetKey)}"
      type="button"
      style="left:${entry.x}px;top:${entry.y}px;"
    >
      <span class="system-overview-entry-label">${escapeHtml(entry.label)}</span>
    </button>
  `;
}

function buildSystemOverviewVisualModel(nodes, { positions = state.positions } = {}) {
  const availableTargetKeys = new Set(
    nodes
      .map((node) => node?.paths?.targetKey || null)
      .filter(Boolean)
  );
  const entries = SYSTEM_OVERVIEW_ENTRY_LAYOUT
    .filter((entry) => availableTargetKeys.has(entry.targetKey))
    .map((entry) => {
      const position = positions?.[entry.id] || { x: entry.x, y: entry.y };
      return {
        ...entry,
        x: position.x,
        y: position.y
      };
    });
  const groups = Array.from(
    entries.reduce((collector, entry) => {
      if (!collector.has(entry.group)) {
        collector.set(entry.group, []);
      }

      collector.get(entry.group).push(entry);
      return collector;
    }, new Map())
  ).map(([key, groupEntries]) => ({
    key,
    stage: groupEntries[0].stage,
    entries: groupEntries,
    bounds: computeEntryBounds(groupEntries)
  }));
  const entryById = new Map(entries.map((entry) => [entry.id, entry]));
  const stages = Object.entries(SYSTEM_OVERVIEW_STAGE_TITLES)
    .map(([key, title]) => {
      const stageGroups = groups.filter((group) => group.stage === key);

      if (!stageGroups.length) {
        return null;
      }

      const bounds = stageGroups.reduce(
        (collector, group) => ({
          minX: Math.min(collector.minX, group.bounds.minX),
          minY: Math.min(collector.minY, group.bounds.minY),
          maxX: Math.max(collector.maxX, group.bounds.maxX),
          maxY: Math.max(collector.maxY, group.bounds.maxY)
        }),
        {
          minX: Number.POSITIVE_INFINITY,
          minY: Number.POSITIVE_INFINITY,
          maxX: Number.NEGATIVE_INFINITY,
          maxY: Number.NEGATIVE_INFINITY
        }
      );

      return {
        key,
        title,
        bounds: {
          minX: Math.round(bounds.minX - SYSTEM_OVERVIEW_STAGE_PADDING_X),
          minY: Math.round(bounds.minY - getSystemOverviewStagePaddingY(key)),
          width: Math.round(bounds.maxX - bounds.minX + SYSTEM_OVERVIEW_STAGE_PADDING_X * 2),
          height: Math.round(bounds.maxY - bounds.minY + getSystemOverviewStagePaddingY(key) * 2)
        },
        labelX: Math.round((bounds.minX + bounds.maxX) / 2),
        labelY: Math.round(bounds.minY - 88)
      };
    })
    .filter(Boolean);
  const stageByKey = new Map(stages.map((stage) => [stage.key, stage]));
  const connectors = SYSTEM_OVERVIEW_CONNECTOR_SPECS
    .map((spec) => buildSystemOverviewConnectorPath(spec, stageByKey, entryById))
    .filter(Boolean);

  return {
    entries,
    stages,
    connectors
  };
}

function buildSystemOverviewConnectorPath(spec, stageByKey, entryById) {
  const start = resolveSystemOverviewAnchor(spec.from, stageByKey, entryById);
  const end = resolveSystemOverviewAnchor(spec.to, stageByKey, entryById);

  if (!start || !end) {
    return "";
  }
  const distance = Math.max(120, end.x - start.x);
  const controlOffset = Math.max(96, Math.round(distance * 0.44));

  return `M ${start.x} ${start.y} C ${start.x + controlOffset} ${start.y}, ${end.x - controlOffset} ${end.y}, ${end.x} ${end.y}`;
}

function resolveSystemOverviewAnchor(anchor, stageByKey, entryById) {
  if (!anchor) {
    return null;
  }

  if (anchor.type === "entry") {
    const entry = entryById.get(anchor.key);

    if (!entry) {
      return null;
    }

    return {
      x: anchor.side === "left" ? entry.x : entry.x + SYSTEM_OVERVIEW_ENTRY_WIDTH,
      y: Math.round(entry.y + SYSTEM_OVERVIEW_ENTRY_HEIGHT / 2)
    };
  }

  const stage = stageByKey.get(anchor.key);

  if (!stage) {
    return null;
  }

  const alignedEntry = anchor.alignEntryId ? entryById.get(anchor.alignEntryId) : null;
  return {
    x: anchor.side === "left" ? stage.bounds.minX : stage.bounds.minX + stage.bounds.width,
    y: alignedEntry
      ? Math.round(alignedEntry.y + SYSTEM_OVERVIEW_ENTRY_HEIGHT / 2)
      : Math.round(stage.bounds.minY + stage.bounds.height / 2)
  };
}

function computeEntryBounds(entries) {
  return entries.reduce(
    (bounds, entry) => ({
      minX: Math.min(bounds.minX, entry.x),
      minY: Math.min(bounds.minY, entry.y),
      maxX: Math.max(bounds.maxX, entry.x + SYSTEM_OVERVIEW_ENTRY_WIDTH),
      maxY: Math.max(bounds.maxY, entry.y + SYSTEM_OVERVIEW_ENTRY_HEIGHT)
    }),
    {
      minX: Number.POSITIVE_INFINITY,
      minY: Number.POSITIVE_INFINITY,
      maxX: Number.NEGATIVE_INFINITY,
      maxY: Number.NEGATIVE_INFINITY
    }
  );
}

function getSystemOverviewStagePaddingY(stageKey) {
  if (stageKey === "claimsNormalization") {
    return SYSTEM_OVERVIEW_NORMALIZATION_STAGE_PADDING_Y;
  }

  return SYSTEM_OVERVIEW_STAGE_PADDING_Y;
}

function isSystemOverviewVisibleNode(node) {
  const targetKey = node?.paths?.targetKey || "";
  return SYSTEM_OVERVIEW_ENTRY_LAYOUT.some((entry) => entry.targetKey === targetKey);
}

function getSystemOverviewStageForTargetKey(targetKey) {
  return SYSTEM_OVERVIEW_ENTRY_LAYOUT.find((entry) => entry.targetKey === targetKey)?.stage || "dataMarts";
}

function formatSystemOverviewLabel(targetKey) {
  if (!targetKey) {
    return "";
  }

  return targetKey.replace(/^input_layer__/, "");
}

function renderDagLauncher() {
  const currentTarget = getCurrentTarget();

  return `
    <div class="dag-launcher ${state.launcherOpen ? "is-open" : ""}">
      <button class="dag-launcher-trigger" id="dag-launcher-trigger" type="button" aria-expanded="${String(state.launcherOpen)}">
        <span class="dag-launcher-copy">
          <span class="dag-launcher-title">${escapeHtml(currentTarget?.label || state.payload?.target?.label || "Select DAG")}</span>
        </span>
        <span class="dag-launcher-caret" aria-hidden="true">${state.launcherOpen ? "Close" : "Browse"}</span>
      </button>
      ${state.launcherOpen ? renderTargetPanel() : ""}
    </div>
  `;
}

function renderHeaderActions() {
  const actions = [];

  if (!IS_STATIC_MODE) {
    actions.push(`
      <button
        class="dag-header-action"
        id="refresh-button"
        type="button"
        ${state.refresh.status === "refreshing" ? "disabled" : ""}
      >
        ${state.refresh.status === "refreshing" ? "Refreshing..." : "Refresh"}
      </button>
    `);
  }

  actions.push(renderLegend());
  actions.push('<button class="dag-header-action" id="clear-button" type="button">Reset</button>');

  return actions.join("");
}

function renderTargetPanel() {
  const featuredTarget = state.targets.find((target) => target.key === SYSTEM_OVERVIEW_TARGET_KEY) || null;
  const categories = [
    { label: "Input Layer", targets: getTargetsForCategory("Input Layer") },
    { label: "Claims Normalization", targets: getTargetsForCategory("Claims Normalization") },
    { label: "Claims Preprocessing", targets: getTargetsForCategory("Claims Preprocessing") },
    { label: "Core", targets: getTargetsForCategory("Core") },
    { label: "Data Marts", targets: getTargetsForCategory("Data Marts") }
  ].filter((category) => category.targets.length);

  return `
    <div class="target-panel" id="target-panel">
      ${
        featuredTarget
          ? `
            <section class="target-panel-featured">
              <h2>Overview</h2>
              <div class="target-panel-list">
                ${renderTargetButton(featuredTarget)}
              </div>
            </section>
          `
          : ""
      }
      <div class="target-panel-grid">
      ${categories
        .map((category) => {
          return `
            <section class="target-panel-column">
              <h2>${escapeHtml(category.label)}</h2>
              <div class="target-panel-list">
                ${category.targets.map(renderTargetButton).join("")}
              </div>
            </section>
          `;
        })
        .join("")}
      </div>
    </div>
  `;
}

function renderTargetButton(target) {
  const isActive = target.key === state.payload?.target?.key;

  return `
    <button
      class="target-panel-item ${isActive ? "is-active" : ""}"
      data-target-key="${escapeAttribute(target.key)}"
      type="button"
    >
      ${escapeHtml(target.label)}
    </button>
  `;
}

function renderModal(node) {
  const editor = getEditorState(node);
  const editable = canEditNode(node);
  const sections = [];
  const modalTabs = getModalTabs(node);
  const activeModalTab = resolveActiveModalTab(node, modalTabs);

  if (isSeedNode(node)) {
    sections.push(
      {
        tab: "overview",
        html: renderEditableTextSection({
          title: "Table Description",
          value: renderTableDescription(node, editor),
          editorValue: editor.draft.description,
          fieldKey: "description",
          editable,
          isEditing: editor.isEditing,
          multiline: true,
          sectionClass: "modal-section--description"
        })
      }
    );
    sections.push({
      tab: "overview",
      html: renderSeedPreviewSection(node, "modal-section--seed")
    });
  } else if (isInputDocumentationNode(node)) {
    sections.push(
      {
        tab: "overview",
        html: renderEditableTextSection({
          title: "Table Description",
          value: renderTableDescription(node, editor),
          editorValue: editor.draft.description,
          fieldKey: "description",
          editable,
          isEditing: editor.isEditing,
          multiline: true,
          sectionClass: "modal-section--description"
        })
      }
    );
    sections.push(
      {
        tab: "overview",
        html: renderEditableTextSection({
          title: "Table Grain",
          value: renderGrainAndPrimaryKey(node, editor),
          editorValue: editor.draft.grain,
          fieldKey: "grain",
          editable,
          isEditing: editor.isEditing,
          multiline: true,
          sectionClass: "modal-section--grain"
        })
      }
    );
    sections.push({
      tab: "dictionary",
      html: renderDictionarySection(node, editor, "modal-section--dictionary")
    });
  } else if (isDagBoundaryNode(node)) {
    sections.push(
      {
        tab: "overview",
        html: renderModalSection(
          "Transformation Description",
          renderTransformationDescription(node, editor),
          true,
          "modal-section--transformation"
        )
      }
    );
    sections.push({
      tab: "overview",
      html: renderModalSection("Table Description", renderTableDescription(node, editor), false, "modal-section--description")
    });
    sections.push({
      tab: "overview",
      html: renderModalSection("Table Grain", renderGrainAndPrimaryKey(node, editor), false, "modal-section--grain")
    });
  } else {
    sections.push(
      {
        tab: "overview",
        html: renderEditableTextSection({
          title: "Transformation Description",
          valueHtml: renderTransformationDescription(node, editor),
          editorValue: editor.draft.transformationStepsText,
          fieldKey: "transformationStepsText",
          editable,
          isEditing: editor.isEditing,
          multiline: true,
          isHtml: true,
          sectionClass: "modal-section--transformation"
        })
      }
    );
    sections.push(
      {
        tab: "overview",
        html: renderEditableTextSection({
          title: "Table Description",
          value: renderTableDescription(node, editor),
          editorValue: editor.draft.description,
          fieldKey: "description",
          editable,
          isEditing: editor.isEditing,
          multiline: true,
          sectionClass: "modal-section--description"
        })
      }
    );
    sections.push(
      {
        tab: "overview",
        html: renderEditableTextSection({
          title: "Table Grain",
          value: renderGrainAndPrimaryKey(node, editor),
          editorValue: editor.draft.grain,
          fieldKey: "grain",
          editable,
          isEditing: editor.isEditing,
          multiline: true,
          sectionClass: "modal-section--grain"
        })
      }
    );
    sections.push({
      tab: "dictionary",
      html: renderDictionarySection(node, editor, "modal-section--dictionary")
    });
    sections.push({
      tab: "sql",
      html: renderSqlSection(node, editor, "modal-section--sql")
    });
  }

  const visibleSections = sections.filter((section) => section.tab === activeModalTab).map((section) => section.html);
  const modalBodyClass = getModalBodyClass(node, activeModalTab);

  return `
    <div class="modal-backdrop" id="modal-backdrop">
      <div class="modal-card" id="modal-card" role="dialog" aria-modal="true" aria-labelledby="modal-title">
        <header class="modal-header">
          <div>
            <div class="modal-kicker">${escapeHtml(getNodeHeaderSchema(node))}</div>
            <h2 id="modal-title">${escapeHtml(getNodeHeaderTable(node))}</h2>
            <div class="modal-subtitle">${escapeHtml(getNodeHeaderPath(node))}</div>
          </div>
          <div class="modal-actions">
            ${
              editable
                ? `<button class="modal-save" id="modal-save" type="button" ${editor.dirty && !editor.isSaving ? "" : "disabled"} ${
                    editor.dirty || editor.isSaving ? "" : "hidden"
                  }>
                    ${editor.isSaving ? "Saving..." : "Save"}
                  </button>`
                : ""
            }
            ${
              editable
                ? `<button class="modal-edit-toggle ${editor.isEditing ? "is-active" : ""}" id="modal-edit-toggle" type="button">
                    ${editor.isEditing ? "Editing" : "Edit"}
                  </button>`
                : ""
            }
            <button class="modal-close" id="modal-close" type="button">Close</button>
          </div>
        </header>
        ${modalTabs.length > 1 ? renderModalTabs(modalTabs, activeModalTab) : ""}
        <div class="${escapeAttribute(modalBodyClass)}">
          ${renderEditorNotice(editor)}
          ${visibleSections.join("")}
        </div>
      </div>
    </div>
  `;
}

function renderModalSection(title, content, isHtml = false, sectionClass = "") {
  return `
    <section class="modal-section ${escapeAttribute(sectionClass)}">
      <h3>${escapeHtml(title)}</h3>
      <div class="modal-section-body">${isHtml ? content : `<p>${escapeHtml(content)}</p>`}</div>
    </section>
  `;
}

function renderEditableTextSection({
  title,
  value = "",
  valueHtml = "",
  editorValue = "",
  fieldKey,
  editable = false,
  isEditing = false,
  multiline = true,
  isHtml = false,
  sectionClass = ""
}) {
  if (editable && isEditing) {
    const control = multiline
      ? `<textarea class="editor-textarea" data-editor-field="${escapeAttribute(fieldKey)}">${escapeHtml(editorValue || "")}</textarea>`
      : `<input class="editor-input" data-editor-field="${escapeAttribute(fieldKey)}" value="${escapeAttribute(editorValue || "")}" />`;

    return `
      <section class="modal-section ${escapeAttribute(sectionClass)}">
        <h3>${escapeHtml(title)}</h3>
        <div class="modal-section-body modal-section-body-editing">
          ${control}
        </div>
      </section>
    `;
  }

  return renderSectionPreview({
    title,
    contentHtml: isHtml ? valueHtml : `<p>${escapeHtml(value || "")}</p>`,
    editable,
    fieldKey,
    sectionClass
  });
}

function renderSectionPreview({ title, contentHtml, editable = false, fieldKey = "", sectionClass = "" }) {
  return `
    <section class="modal-section ${editable ? "is-editable-section" : ""} ${escapeAttribute(sectionClass)}">
      <h3>${escapeHtml(title)}</h3>
      <div class="modal-section-body">
        ${contentHtml}
      </div>
    </section>
  `;
}

function renderEditorNotice(editor) {
  if (!editor.error) {
    return "";
  }

  return `<div class="modal-editor-error">${escapeHtml(editor.error)}</div>`;
}

function renderModalTabs(tabs, activeTab) {
  return `
    <div class="modal-tabs" role="tablist" aria-label="Node detail sections">
      ${tabs
        .map((tab) => {
          const isActive = tab.key === activeTab;

          return `
            <button
              class="modal-tab ${isActive ? "is-active" : ""}"
              data-modal-tab="${escapeAttribute(tab.key)}"
              type="button"
              role="tab"
              aria-selected="${String(isActive)}"
            >
              ${escapeHtml(tab.label)}
            </button>
          `;
        })
        .join("")}
    </div>
  `;
}

function getModalBodyClass(node, activeTab) {
  return "modal-body";
}

function getModalTabs(node) {
  if (!node) {
    return [{ key: "overview", label: "Overview" }];
  }

  if (isSeedNode(node)) {
    return [{ key: "overview", label: "Overview" }];
  }

  if (isInputDocumentationNode(node)) {
    return [
      { key: "overview", label: "Overview" },
      { key: "dictionary", label: "Data Dictionary" }
    ];
  }

  if (isDagBoundaryNode(node)) {
    return [{ key: "overview", label: "Overview" }];
  }

  return [
    { key: "overview", label: "Overview" },
    { key: "dictionary", label: "Data Dictionary" },
    { key: "sql", label: "SQL" }
  ];
}

function resolveActiveModalTab(node, tabs) {
  if (!node || !tabs.length) {
    state.modalTabNodeId = null;
    state.modalTab = null;
    return "overview";
  }

  if (state.modalTabNodeId !== node.id) {
    state.modalTabNodeId = node.id;
    state.modalTab = getDefaultModalTab(node, tabs);
    return state.modalTab;
  }

  if (!tabs.some((tab) => tab.key === state.modalTab)) {
    state.modalTab = getDefaultModalTab(node, tabs);
  }

  return state.modalTab || tabs[0].key;
}

function getDefaultModalTab(node, tabs) {
  if (INITIAL_OPEN_NODE_ID === node.id && INITIAL_MODAL_TAB && tabs.some((tab) => tab.key === INITIAL_MODAL_TAB)) {
    return INITIAL_MODAL_TAB;
  }

  if (state.editor?.focusField) {
    const tabFromField = getModalTabForField(node, state.editor.focusField);

    if (tabFromField && tabs.some((tab) => tab.key === tabFromField)) {
      return tabFromField;
    }
  }

  return tabs[0]?.key || "overview";
}

function getModalTabForField(node, fieldKey) {
  if (!fieldKey) {
    return "overview";
  }

  if (fieldKey === "dictionary") {
    return "dictionary";
  }

  if (fieldKey === "sql") {
    return "sql";
  }

  if (fieldKey === "description" || fieldKey === "grain" || fieldKey === "transformationStepsText") {
    return "overview";
  }

  if (isSeedNode(node)) {
    return "overview";
  }

  return "overview";
}

function renderModalSubtitle(node) {
  if (isCollapsedInputBoundary(node)) {
    return `${node.layer} • input table`;
  }

  if (isDagBoundaryNode(node)) {
    return `${node.layer} • collapsed DAG`;
  }

  return `${node.materialized} • ${node.technical.packageName}`;
}

function getNodeHeaderSchema(node) {
  if (isDagBoundaryNode(node)) {
    return node.folderLabel || node.layer || "";
  }

  return node.technical?.schemaName || node.folderLabel || node.layer || "";
}

function getNodeHeaderTable(node) {
  if (isDagBoundaryNode(node)) {
    return node.name || "";
  }

  const baseName = node.technical?.alias || node.name || "";
  const segments = baseName.split("__").filter(Boolean);
  return segments[segments.length - 1] || baseName;
}

function getNodeHeaderPath(node) {
  if (node?.paths?.sql) {
    return formatHeaderPath(node.paths.sql, "/models/");
  }

  if (node?.paths?.yaml) {
    return formatHeaderPath(node.paths.yaml, "/models/");
  }

  if (node?.seedViewer?.csvPath) {
    return formatHeaderPath(node.seedViewer.csvPath, "/seeds/");
  }

  return renderModalSubtitle(node);
}

function formatHeaderPath(filePath, marker) {
  if (!filePath) {
    return "";
  }

  const normalizedPath = String(filePath).replace(/\\/g, "/");
  const markerIndex = normalizedPath.lastIndexOf(marker);
  const relativePath = markerIndex >= 0
    ? normalizedPath.slice(markerIndex + marker.length)
    : normalizedPath.split("/").slice(-3).join("/");

  return relativePath.replace(/\.[^.]+$/, "");
}

function getPreviewDraft(node, editor = getEditorState(node)) {
  if (!node || !editor || editor.nodeId !== node.id || !editor.draft) {
    return null;
  }

  return editor.draft;
}

function renderTransformationDescription(node, editor = getEditorState(node)) {
  if (isDagBoundaryNode(node)) {
    const outputModels = node.dagBoundary?.outputModels?.length
      ? `Outputs in this collapsed DAG: ${node.dagBoundary.outputModels.join(", ")}.`
      : "";

    return `<p>${escapeHtml(
      `This node stands in for the ${node.name} DAG so the current canvas stays readable. ${outputModels}`.trim()
    )}</p>`;
  }

  const draft = getPreviewDraft(node, editor);

  if (draft) {
    if (draft.transformationStepsText.trim()) {
      return `<p>${escapeHtml(draft.transformationStepsText.trimEnd())}</p>`;
    }

    return "<p>No curated transformation description has been added yet.</p>";
  }

  if (Array.isArray(node.curated.transformationSteps) && node.curated.transformationSteps.length) {
    return `
      <ol class="simple-list">
        ${node.curated.transformationSteps.map((step) => `<li>${escapeHtml(step)}</li>`).join("")}
      </ol>
    `;
  }

  if (typeof node.curated.transformationSteps === "string" && node.curated.transformationSteps.trim()) {
    return `<p>${escapeHtml(node.curated.transformationSteps.trimEnd())}</p>`;
  }

  const sentences = [];

  if (node.mainDependencies.length) {
    sentences.push(`Reads from ${node.mainDependencies.map((dependency) => dependency.name).join(", ")}.`);
  }

  if (node.supportingDependencies.length) {
    sentences.push(`Also references ${node.supportingDependencies.map((dependency) => dependency.name).join(", ")}.`);
  }

  if (!sentences.length) {
    sentences.push("No curated transformation description has been added yet.");
  }

  return `<p>${escapeHtml(sentences.join(" "))}</p>`;
}

function renderSeedPreviewSection(node, sectionClass = "") {
  const preview = getSeedPreviewState(node.id);
  const totalPages = Math.max(1, Math.ceil((preview.totalMatches || 0) / preview.pageSize));
  const isLoading = preview.status === "loading";
  const canGoPrevious = preview.page > 1;
  const canGoNext = preview.page < totalPages;
  const downloadUrl = node.seedViewer?.downloadUrl || "";

  return `
    <section class="modal-section ${escapeAttribute(sectionClass)}">
      <h3>Seed Data</h3>
      <div class="modal-section-body">
        <div class="seed-viewer">
          <div class="seed-toolbar">
            <label class="seed-search" for="seed-search-input">
              <span>Search rows</span>
              <input
                id="seed-search-input"
                type="search"
                placeholder="Search this seed"
                value="${escapeAttribute(preview.query)}"
              />
            </label>
            ${
              downloadUrl
                ? `<a class="seed-download" href="${escapeAttribute(downloadUrl)}" target="_blank" rel="noreferrer">
                    Download CSV
                  </a>`
                : ""
            }
          </div>

          <div class="seed-meta">
            ${
              isLoading
                ? "Loading seed rows..."
                : `Showing ${preview.rows.length} of ${preview.totalMatches} matching rows (${preview.totalRows} total).`
            }
          </div>

          ${
            preview.error
              ? `<p class="seed-error">${escapeHtml(preview.error)}</p>`
              : renderSeedPreviewTable(preview)
          }

          <div class="seed-pagination">
            <button class="seed-page-button" id="seed-page-prev" type="button" ${canGoPrevious ? "" : "disabled"}>
              Previous
            </button>
            <div class="seed-page-copy">Page ${preview.page} of ${totalPages}</div>
            <button class="seed-page-button" id="seed-page-next" type="button" ${canGoNext ? "" : "disabled"}>
              Next
            </button>
          </div>
        </div>
      </div>
    </section>
  `;
}

function renderSeedPreviewTable(preview) {
  if (!preview.rows.length) {
    return `<p class="seed-empty">${escapeHtml(
      preview.query ? "No rows match the current search." : "No rows were returned for this seed."
    )}</p>`;
  }

  return `
    <div class="table-wrap seed-table-wrap">
      <table class="dictionary-table seed-preview-table">
        <thead>
          <tr>
            ${preview.headers.map((header) => `<th>${escapeHtml(header)}</th>`).join("")}
          </tr>
        </thead>
        <tbody>
          ${preview.rows
            .map((row) => {
              return `<tr>${row.map((cell) => `<td>${escapeHtml(String(cell ?? ""))}</td>`).join("")}</tr>`;
            })
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderTableDescription(node, editor = getEditorState(node)) {
  if (isDagBoundaryNode(node) && !isCollapsedInputBoundary(node)) {
    return node.description || `Collapsed DAG boundary for ${node.name}.`;
  }

  const draft = getPreviewDraft(node, editor);
  const parts = [];

  if (node.curated.whatItRepresents) {
    parts.push(node.curated.whatItRepresents);
  }

  const description = draft ? draft.description : node.description;

  if (description) {
    parts.push(description);
  }

  return parts.join("\n\n") || "No table description has been added yet.";
}

function renderGrainAndPrimaryKey(node, editor = getEditorState(node)) {
  if (isDagBoundaryNode(node) && !isCollapsedInputBoundary(node)) {
    const outputs = node.dagBoundary?.outputModels?.length
      ? node.dagBoundary.outputModels.join(", ")
      : "No output models recorded.";
    const memberCount = node.dagBoundary?.memberCount || 0;

    return `Represents the ${node.name} DAG boundary in this view.\n\nContained models: ${memberCount}\n\nOutput models: ${outputs}`;
  }

  const draft = getPreviewDraft(node, editor);
  const grain = draft ? draft.grain || "Not yet authored." : node.curated.grain || "Not yet authored.";
  const draftPrimaryKeys = draft ? draft.columns.filter((column) => column.isPrimaryKey).map((column) => column.name) : [];
  const primaryKey = draftPrimaryKeys.length
    ? draftPrimaryKeys.join(", ")
    : formatCuratedPrimaryKey(node.curated.primaryKey) ||
      (node.technical.primaryKeyColumns.length ? node.technical.primaryKeyColumns.join(", ") : "Not declared.");

  return `Table grain: ${grain}\n\nPrimary key: ${primaryKey}`;
}

function shouldRenderTransformationSection(node) {
  if (isDagBoundaryNode(node)) {
    return !isCollapsedInputBoundary(node);
  }

  if (isInputDocumentationNode(node)) {
    return false;
  }

  if (isSeedNode(node)) {
    return false;
  }

  return true;
}

function isSeedNode(node) {
  return node?.resourceType === "seed";
}

function isDagBoundaryNode(node) {
  return node?.resourceType === "dag";
}

function isInputNode(node) {
  return node?.nodeType === "input";
}

function isCollapsedInputBoundary(node) {
  return isDagBoundaryNode(node) && isInputNode(node);
}

function isInputDocumentationNode(node) {
  return isInputNode(node) || isCollapsedInputBoundary(node);
}

function isSourceNode(node) {
  return Boolean(node?.sourceStyle);
}

function getSeedPreviewState(nodeId) {
  const existing = state.seedPreviewByNodeId[nodeId];

  if (existing) {
    return existing;
  }

  return {
    status: "idle",
    query: "",
    page: 1,
    pageSize: SEED_PREVIEW_PAGE_SIZE,
    totalRows: 0,
    totalMatches: 0,
    headers: [],
    rows: [],
    error: null,
    requestToken: 0
  };
}

function renderDictionarySection(node, editor = getEditorState(node), sectionClass = "") {
  const includeMappingInstructions = isSourceNode(node);
  const isEditing = canEditNode(node) && editor.isEditing;
  const previewColumns = getPreviewDraft(node, editor)?.columns || node.columns;

  if (isEditing) {
    return `
      <section class="modal-section ${escapeAttribute(sectionClass)}">
        <h3>Data Dictionary</h3>
        <div class="table-wrap">
          <table class="dictionary-table">
            <thead>
              <tr>
                <th>Column</th>
                <th>Type</th>
                <th>Description</th>
                ${includeMappingInstructions ? "<th>Mapping Instructions</th>" : ""}
                <th>PK</th>
              </tr>
            </thead>
            <tbody>
              ${editor.draft.columns
                .map((column, index) => renderEditableDictionaryRow(column, index, { includeMappingInstructions }))
                .join("")}
            </tbody>
          </table>
        </div>
      </section>
    `;
  }

  return renderSectionPreview({
    title: "Data Dictionary",
    editable: canEditNode(node),
    fieldKey: "dictionary",
    sectionClass,
    contentHtml: `
      <div class="table-wrap">
        <table class="dictionary-table">
          <thead>
            <tr>
              <th>Column</th>
              <th>Type</th>
              <th>Description</th>
              ${includeMappingInstructions ? "<th>Mapping Instructions</th>" : ""}
            </tr>
          </thead>
          <tbody>
            ${previewColumns.map((column) => renderDictionaryRow(column, { includeMappingInstructions })).join("")}
          </tbody>
        </table>
      </div>
    `
  });
}

function renderDictionaryRow(column, { includeMappingInstructions = false } = {}) {
  return `
    <tr>
      <td>
        <div class="dictionary-column">${escapeHtml(column.name)}</div>
        ${column.isPrimaryKey ? `<div class="dictionary-badge">PK</div>` : ""}
      </td>
      <td>${escapeHtml(column.dataType || "Unknown")}</td>
      <td>${escapeHtml(column.description || "No description yet.")}</td>
      ${includeMappingInstructions ? `<td>${escapeHtml(column.mappingInstructions || "")}</td>` : ""}
    </tr>
  `;
}

function renderEditableDictionaryRow(column, index, { includeMappingInstructions = false } = {}) {
  return `
    <tr>
      <td>
        <div class="dictionary-column">${escapeHtml(column.name)}</div>
      </td>
      <td>
        <input
          class="editor-input editor-input-table"
          data-editor-column-index="${index}"
          data-editor-column-prop="dataType"
          value="${escapeAttribute(column.dataType || "")}"
        />
      </td>
      <td>
        <textarea
          class="editor-textarea editor-textarea-table"
          data-editor-column-index="${index}"
          data-editor-column-prop="description"
        >${escapeHtml(column.description || "")}</textarea>
      </td>
      ${
        includeMappingInstructions
          ? `<td>
              <textarea
                class="editor-textarea editor-textarea-table"
                data-editor-column-index="${index}"
                data-editor-column-prop="mappingInstructions"
              >${escapeHtml(column.mappingInstructions || "")}</textarea>
            </td>`
          : ""
      }
      <td>
        <label class="editor-checkbox">
          <input
            type="checkbox"
            data-editor-column-index="${index}"
            data-editor-column-prop="isPrimaryKey"
            ${column.isPrimaryKey ? "checked" : ""}
          />
          <span>PK</span>
        </label>
      </td>
    </tr>
  `;
}

function renderSqlSection(node, editor = getEditorState(node), sectionClass = "") {
  if (editor.isEditing && canEditNode(node)) {
    return `
      <section class="modal-section ${escapeAttribute(sectionClass)}">
        <h3>SQL</h3>
        <div class="modal-section-body modal-section-body-editing">
          <textarea class="editor-textarea editor-textarea-sql" data-editor-field="sql">${escapeHtml(
            editor.draft.sql || ""
          )}</textarea>
        </div>
      </section>
    `;
  }

  return renderSectionPreview({
    title: "SQL",
    editable: canEditNode(node),
    fieldKey: "sql",
    sectionClass,
    contentHtml: `<div class="sql-block"><code>${escapeHtml(getPreviewDraft(node, editor)?.sql || node.sql)}</code></div>`
  });
}

function renderRefreshErrorNotice() {
  if (IS_STATIC_MODE) {
    return "";
  }

  if (state.refresh.status !== "failed" || !state.refresh.lastError) {
    return "";
  }

  return `
    <details class="error-toast">
      <summary>Refresh failed. The last good DAG is still on screen.</summary>
      <div class="error-meta">
        ${state.refresh.lastError.command ? `<div><strong>Command:</strong> ${escapeHtml(state.refresh.lastError.command)}</div>` : ""}
        ${
          state.refresh.lastError.exitCode !== null
            ? `<div><strong>Exit code:</strong> ${escapeHtml(String(state.refresh.lastError.exitCode))}</div>`
            : ""
        }
      </div>
      <pre class="error-output">${escapeHtml(
        state.refresh.lastError.detail || state.refresh.lastError.message || ""
      )}</pre>
    </details>
  `;
}

function renderLegend() {
  return `
    <details class="header-legend">
      <summary class="header-legend-trigger dag-header-action">Legend</summary>
      <div class="header-legend-panel">
        <div class="legend-items">
        ${renderLegendItem("Input Table", "input")}
        ${renderLegendItem("Transformation Model", "intermediate")}
        ${renderLegendItem("Output Table", "output")}
        ${renderLegendItem("Terminology / Value Set", "terminology")}
        </div>
      </div>
    </details>
  `;
}

function renderLegendItem(label, nodeType) {
  return `
    <div class="legend-item">
      <span class="legend-swatch" style="background:${resolveNodeColor(nodeType)}"></span>
      <span>${escapeHtml(label)}</span>
    </div>
  `;
}

function renderStatusPage() {
  const refresh = state.refresh;

  app.innerHTML = `
    <div class="status-page">
      <div class="status-card">
        <h1>${escapeHtml(renderStatusPageTitle(refresh))}</h1>
        <p>${escapeHtml(renderRefreshSummary(refresh))}</p>
        ${
          IS_STATIC_MODE
            ? ""
            : `<button
                class="hud-button"
                id="refresh-button"
                type="button"
                ${refresh.status === "refreshing" ? "disabled" : ""}
              >
                ${refresh.status === "refreshing" ? "Refreshing..." : "Refresh DAG"}
              </button>`
        }
        ${renderStatusPageError(refresh)}
      </div>
    </div>
  `;

  bindHudEvents();
}

function renderStatusPageError(refresh) {
  if (refresh.status !== "failed" || !refresh.lastError) {
    return "";
  }

  return `<pre class="error-output">${escapeHtml(refresh.lastError.detail || refresh.lastError.message || "")}</pre>`;
}

function renderFatalError(error) {
  const message = error instanceof Error ? error.message : String(error);

  app.innerHTML = `
    <div class="status-page">
      <div class="status-card">
        <h1>Unable to load the app</h1>
        <p>${escapeHtml(message)}</p>
      </div>
    </div>
  `;
}

function bindHudEvents() {
  document.querySelector("#refresh-button")?.addEventListener("click", handleRefreshClick);
  document.querySelector("#clear-button")?.addEventListener("click", handleClearClick);
  document.querySelector("#fit-button")?.addEventListener("click", handleFitClick);
  document.querySelector("#zoom-in-button")?.addEventListener("click", () => zoomCanvas(1.12));
  document.querySelector("#zoom-out-button")?.addEventListener("click", () => zoomCanvas(1 / 1.12));
  document.querySelector("#dag-launcher-trigger")?.addEventListener("click", toggleLauncher);
  document.querySelector("#launcher-backdrop")?.addEventListener("click", closeLauncher);
  document.querySelectorAll(".target-panel-item").forEach((button) => {
    button.addEventListener("click", handleTargetItemClick);
  });
}

function bindCanvasEvents() {
  const stage = document.querySelector("#canvas-stage");

  if (!stage) {
    return;
  }

  stage.addEventListener("pointerdown", handleStagePointerDown);
  stage.addEventListener("wheel", handleStageWheel, { passive: false });

  document.querySelectorAll(".dag-node").forEach((nodeElement) => {
    nodeElement.addEventListener("pointerdown", handleNodePointerDown);
  });

  document.querySelectorAll(".system-overview-entry").forEach((entryElement) => {
    entryElement.addEventListener("click", handleSystemOverviewEntryClick);
  });

  document.querySelectorAll(".system-overview-stage-frame, .system-overview-stage-label").forEach((element) => {
    element.addEventListener("pointerdown", handleSystemOverviewStagePointerDown);
  });
}

function bindModalEvents() {
  const closeButton = document.querySelector("#modal-close");
  const backdrop = document.querySelector("#modal-backdrop");
  const saveButton = document.querySelector("#modal-save");
  const editToggleButton = document.querySelector("#modal-edit-toggle");
  const modalCard = document.querySelector("#modal-card");

  closeButton?.addEventListener("click", closeModal);
  saveButton?.addEventListener("click", handleSaveNodeEdits);
  editToggleButton?.addEventListener("click", toggleEditingMode);

  backdrop?.addEventListener("click", (event) => {
    if (event.target === backdrop) {
      closeModal();
    }
  });

  modalCard?.querySelectorAll("[data-modal-tab]").forEach((element) => {
    element.addEventListener("click", () => {
      state.modalTabNodeId = state.openNodeId;
      state.modalTab = element.dataset.modalTab || "overview";
      render();
    });
  });

  modalCard?.querySelectorAll("[data-editor-field]").forEach((element) => {
    element.addEventListener("input", handleEditorInput);
  });

  modalCard?.querySelectorAll("[data-editor-column-prop]").forEach((element) => {
    const eventName = element.type === "checkbox" ? "change" : "input";
    element.addEventListener(eventName, handleEditorColumnInput);
  });

  if (state.editor.isEditing && state.editor.focusField) {
    requestAnimationFrame(() => {
      focusEditorField(state.editor.focusField);
    });
  }
}

function bindSeedPreviewEvents(openNode) {
  if (!openNode || !isSeedNode(openNode)) {
    return;
  }

  document.querySelector("#seed-search-input")?.addEventListener("input", (event) => {
    const query = event.target.value;
    const preview = getSeedPreviewState(openNode.id);

    state.seedPreviewByNodeId[openNode.id] = {
      ...preview,
      query,
      page: 1,
      error: null
    };

    requestSeedPreviewDebounced(openNode.id, query);
  });

  document.querySelector("#seed-page-prev")?.addEventListener("click", () => {
    const preview = getSeedPreviewState(openNode.id);

    if (preview.page > 1) {
      requestSeedPreview(openNode.id, {
        query: preview.query,
        page: preview.page - 1
      });
    }
  });

  document.querySelector("#seed-page-next")?.addEventListener("click", () => {
    const preview = getSeedPreviewState(openNode.id);
    const totalPages = Math.max(1, Math.ceil((preview.totalMatches || 0) / preview.pageSize));

    if (preview.page < totalPages) {
      requestSeedPreview(openNode.id, {
        query: preview.query,
        page: preview.page + 1
      });
    }
  });
}

function startEditing(focusField = null) {
  const openNode = getOpenNode();

  if (!canEditNode(openNode)) {
    return;
  }

  if (state.editor.nodeId !== openNode.id) {
    state.editor = createEditorStateForNode(openNode);
  }

  state.editor.isEditing = true;
  state.editor.focusField = focusField;
  state.editor.error = null;
  state.modalTabNodeId = openNode.id;
  state.modalTab = getModalTabForField(openNode, focusField);
  render();
}

function toggleEditingMode() {
  if (state.editor.isEditing) {
    exitEditingMode();
    return;
  }

  startEditing();
}

function exitEditingMode() {
  if (!state.editor.isEditing || state.editor.isSaving) {
    return;
  }

  if (state.editor.dirty && !confirmDiscardEditorChanges()) {
    return;
  }

  state.editor.isEditing = false;
  state.editor.focusField = null;
  state.editor.error = null;
  render();
}

function focusEditorField(fieldKey) {
  const selector = fieldKey === "dictionary"
    ? "[data-editor-column-index='0'][data-editor-column-prop='description'], [data-editor-column-index='0'][data-editor-column-prop='dataType']"
    : `[data-editor-field="${CSS.escape(fieldKey)}"]`;
  const field = document.querySelector(selector);

  if (!field) {
    return;
  }

  field.focus();

  if (typeof field.select === "function") {
    field.select();
  }
}

function handleEditorInput(event) {
  if (!state.editor.draft) {
    return;
  }

  const fieldKey = event.currentTarget.dataset.editorField;
  state.editor.draft[fieldKey] = event.currentTarget.value;
  setEditorDirtyFlag();
  syncEditorChrome();
}

function handleEditorColumnInput(event) {
  if (!state.editor.draft) {
    return;
  }

  const rowIndex = Number(event.currentTarget.dataset.editorColumnIndex);
  const property = event.currentTarget.dataset.editorColumnProp;
  const row = state.editor.draft.columns[rowIndex];

  if (!row || !property) {
    return;
  }

  row[property] = event.currentTarget.type === "checkbox" ? event.currentTarget.checked : event.currentTarget.value;
  setEditorDirtyFlag();
  syncEditorChrome();
}

function syncEditorChrome() {
  const saveButton = document.querySelector("#modal-save");
  const editToggleButton = document.querySelector("#modal-edit-toggle");
  const errorContainer = document.querySelector(".modal-editor-error");

  if (saveButton) {
    saveButton.disabled = !state.editor.dirty || state.editor.isSaving;
    saveButton.textContent = state.editor.isSaving ? "Saving..." : "Save";
    saveButton.hidden = !state.editor.dirty && !state.editor.isSaving;
  }

  if (editToggleButton) {
    editToggleButton.textContent = state.editor.isEditing ? "Editing" : "Edit";
    editToggleButton.classList.toggle("is-active", state.editor.isEditing);
  }

  if (errorContainer) {
    errorContainer.textContent = state.editor.error || "";
    errorContainer.hidden = !state.editor.error;
  }
}

async function handleSaveNodeEdits() {
  const openNode = getOpenNode();

  if (!openNode || !canEditNode(openNode) || !state.editor.dirty || state.editor.isSaving) {
    return;
  }

  state.editor.isSaving = true;
  state.editor.error = null;
  render();

  try {
    const response = await fetch(SAVE_NODE_API_URL, {
      method: "POST",
      credentials: "include",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        targetKey: state.payload?.target?.key || "appointment",
        nodeId: openNode.id,
        changes: buildSaveChanges(openNode, state.editor)
      })
    });
    const body = await response.json();

    if (!response.ok) {
      throw new Error(body.detail || body.error || `Save failed with status ${response.status}`);
    }

    state.editor = createEmptyEditorState();
    applyLineageResponse(body, { preserveSelection: true });
  } catch (error) {
    state.editor.isSaving = false;
    state.editor.error = error instanceof Error ? error.message : String(error);
    render();
  }
}

function buildSaveChanges(node, editor) {
  const changes = {};
  const initial = editor.initialDraft;
  const draft = editor.draft;

  if (draft.description !== initial.description) {
    changes.description = draft.description;
  }

  if (draft.grain !== initial.grain) {
    changes.grain = draft.grain;
  }

  if (!isInputNode(node) && !isSeedNode(node) && draft.transformationStepsText !== initial.transformationStepsText) {
    changes.transformationStepsText = draft.transformationStepsText;
  }

  if (!isInputNode(node) && !isSeedNode(node) && draft.sql !== initial.sql) {
    changes.sql = draft.sql;
  }

  const changedColumns = draft.columns
    .map((column, index) => {
      const previous = initial.columns[index];

      if (!previous) {
        return column;
      }

      if (
        column.description !== previous.description ||
        column.dataType !== previous.dataType ||
        column.mappingInstructions !== previous.mappingInstructions ||
        column.isPrimaryKey !== previous.isPrimaryKey
      ) {
        return column;
      }

      return null;
    })
    .filter(Boolean);

  if (changedColumns.length) {
    changes.columns = changedColumns;
  }

  return changes;
}

function primeOpenNodeData(openNode) {
  if (openNode && isSeedNode(openNode)) {
    const preview = getSeedPreviewState(openNode.id);

    if (preview.status === "idle") {
      void requestSeedPreview(openNode.id, {
        query: preview.query,
        page: preview.page
      });
    }
  }
}

async function requestSeedPreview(nodeId, { query = "", page = 1 } = {}) {
  const node = state.payload?.nodes.find((candidate) => candidate.id === nodeId);

  if (!node || !isSeedNode(node)) {
    return;
  }

  const previous = getSeedPreviewState(nodeId);
  const requestToken = previous.requestToken + 1;

  state.seedPreviewByNodeId[nodeId] = {
    ...previous,
    query,
    page,
    status: "loading",
    error: null,
    requestToken
  };

  render();

  if (IS_STATIC_MODE) {
    state.seedPreviewByNodeId[nodeId] = {
      ...state.seedPreviewByNodeId[nodeId],
      status: "failed",
      error: "Seed preview is not available in the docs snapshot.",
      rows: [],
      totalMatches: 0
    };
    render();
    return;
  }

  try {
    const url = new URL(SEED_PREVIEW_API_URL, window.location.href);
    url.searchParams.set("targetKey", state.payload?.target?.key || "appointment");
    url.searchParams.set("nodeId", nodeId);
    url.searchParams.set("query", query);
    url.searchParams.set("page", String(page));
    url.searchParams.set("pageSize", String(previous.pageSize || SEED_PREVIEW_PAGE_SIZE));

    const response = await fetch(url, { cache: "no-store", credentials: "include" });
    const body = await response.json();

    if (!response.ok) {
      throw new Error(body.error || `Seed preview failed with status ${response.status}`);
    }

    const current = getSeedPreviewState(nodeId);

    if (current.requestToken !== requestToken) {
      return;
    }

    state.seedPreviewByNodeId[nodeId] = {
      ...current,
      status: "ready",
      error: null,
      query: body.query || "",
      page: body.page || 1,
      pageSize: body.pageSize || SEED_PREVIEW_PAGE_SIZE,
      totalRows: body.totalRows || 0,
      totalMatches: body.totalMatches || 0,
      headers: Array.isArray(body.headers) ? body.headers : [],
      rows: Array.isArray(body.rows) ? body.rows : []
    };
    render();
  } catch (error) {
    const current = getSeedPreviewState(nodeId);

    if (current.requestToken !== requestToken) {
      return;
    }

    state.seedPreviewByNodeId[nodeId] = {
      ...current,
      status: "failed",
      error: error instanceof Error ? error.message : String(error),
      rows: [],
      totalMatches: 0
    };
    render();
  }
}

async function changeTarget(nextTargetKey) {
  if (!confirmDiscardEditorChanges()) {
    return;
  }

  if (!nextTargetKey || nextTargetKey === state.payload?.target?.key) {
    closeLauncher();
    return;
  }

  state.launcherOpen = false;
  state.openNodeId = null;
  updateTargetQuery(nextTargetKey);
  await fetchLineage({
    preserveSelection: false,
    targetKey: nextTargetKey
  });
}

async function handleTargetItemClick(event) {
  const nextTargetKey = event.currentTarget.dataset.targetKey;
  await changeTarget(nextTargetKey);
}

async function handleClearClick() {
  if (!confirmDiscardEditorChanges()) {
    return;
  }

  clearStoredLayoutState(SYSTEM_OVERVIEW_TARGET_KEY);
  state.launcherOpen = false;
  state.activeNodeId = null;
  state.openNodeId = null;
  state.modalTabNodeId = null;
  state.modalTab = null;
  state.editor = createEmptyEditorState();
  updateTargetQuery(SYSTEM_OVERVIEW_TARGET_KEY);

  if (state.payload?.target?.key !== SYSTEM_OVERVIEW_TARGET_KEY) {
    await fetchLineage({
      preserveSelection: false,
      targetKey: SYSTEM_OVERVIEW_TARGET_KEY
    });
    return;
  }

  handleFitClick({ persist: false });
}

function toggleLauncher() {
  state.launcherOpen = !state.launcherOpen;
  render();
}

function closeLauncher() {
  if (!state.launcherOpen) {
    return;
  }

  state.launcherOpen = false;
  render();
}

function handleStagePointerDown(event) {
  if (event.button !== 0 || event.target.closest(".dag-node, .system-overview-entry")) {
    return;
  }

  if (state.activeNodeId) {
    state.activeNodeId = null;
    syncScene();
  }

  state.drag = {
    type: "pan",
    startClientX: event.clientX,
    startClientY: event.clientY,
    originX: state.viewport.x,
    originY: state.viewport.y,
    moved: false
  };

  attachDragListeners();
  document.body.classList.add("is-panning");
  document.querySelector("#canvas-stage")?.classList.add("is-panning");
}

async function handleSystemOverviewEntryClick(event) {
  event.stopPropagation();
  const targetKey = event.currentTarget.dataset.targetKey || null;

  if (!targetKey || targetKey === SYSTEM_OVERVIEW_TARGET_KEY) {
    return;
  }

  await changeTarget(targetKey);
}

function handleSystemOverviewStagePointerDown(event) {
  if (event.button !== 0 || !isSystemOverviewActive()) {
    return;
  }

  event.stopPropagation();

  const stageKey = event.currentTarget.dataset.stageKey || null;

  if (!stageKey) {
    return;
  }

  const entryIds = SYSTEM_OVERVIEW_ENTRY_LAYOUT
    .filter((entry) => entry.stage === stageKey)
    .map((entry) => entry.id)
    .filter((entryId) => state.positions[entryId]);

  if (!entryIds.length) {
    return;
  }

  state.drag = {
    type: "overview-stage",
    stageKey,
    entryIds,
    entryOrigins: Object.fromEntries(entryIds.map((entryId) => [entryId, { ...state.positions[entryId] }])),
    startClientX: event.clientX,
    startClientY: event.clientY,
    moved: false
  };

  attachDragListeners();
}

function handleNodePointerDown(event) {
  if (event.button !== 0) {
    return;
  }

  event.stopPropagation();

  const nodeId = event.currentTarget.dataset.nodeId;
  const position = state.positions[nodeId];

  state.activeNodeId = nodeId;
  syncNodeState();

  state.drag = {
    type: "node",
    nodeId,
    startClientX: event.clientX,
    startClientY: event.clientY,
    originX: position.x,
    originY: position.y,
    moved: false
  };

  event.currentTarget.classList.add("is-dragging");
  attachDragListeners();
}

function attachDragListeners() {
  window.addEventListener("pointermove", handlePointerMove);
  window.addEventListener("pointerup", handlePointerUp);
  window.addEventListener("pointercancel", handlePointerUp);
}

function detachDragListeners() {
  window.removeEventListener("pointermove", handlePointerMove);
  window.removeEventListener("pointerup", handlePointerUp);
  window.removeEventListener("pointercancel", handlePointerUp);
}

function handlePointerMove(event) {
  if (!state.drag) {
    return;
  }

  event.preventDefault();

  const dx = event.clientX - state.drag.startClientX;
  const dy = event.clientY - state.drag.startClientY;

  if (Math.abs(dx) > DRAG_THRESHOLD || Math.abs(dy) > DRAG_THRESHOLD) {
    state.drag.moved = true;
  }

  if (state.drag.type === "pan") {
    state.viewport.x = state.drag.originX + dx;
    state.viewport.y = state.drag.originY + dy;
    scheduleViewportSync();
    persistViewportDebounced();
    return;
  }

  if (state.drag.type === "overview-stage") {
    state.drag.entryIds.forEach((entryId) => {
      const origin = state.drag.entryOrigins[entryId];

      if (!origin) {
        return;
      }

      state.positions[entryId] = clampOverviewEntryPosition({
        x: origin.x + dx / state.viewport.scale,
        y: origin.y + dy / state.viewport.scale
      });
    });

    scheduleSceneSync();
    persistPositionsDebounced();
    return;
  }

  if (state.drag.type === "node") {
    const nextX = state.drag.originX + dx / state.viewport.scale;
    const nextY = state.drag.originY + dy / state.viewport.scale;

    state.positions[state.drag.nodeId] = clampPosition({
      x: nextX,
      y: nextY
    });

    scheduleSceneSync();
    persistPositionsDebounced();
  }
}

function handlePointerUp() {
  if (!state.drag) {
    return;
  }

  const drag = state.drag;
  state.drag = null;
  detachDragListeners();

  document.body.classList.remove("is-panning");
  document.querySelector("#canvas-stage")?.classList.remove("is-panning");
  document.querySelectorAll(".dag-node.is-dragging").forEach((element) => {
    element.classList.remove("is-dragging");
  });

  if (drag.type === "pan") {
    storeViewport();
    return;
  }

  if (drag.type === "overview-stage") {
    storePositions();
    return;
  }

  if (!drag.moved) {
    const draggedNode = state.payload?.nodes.find((node) => node.id === drag.nodeId) || null;
    const targetKey = draggedNode?.paths?.targetKey || null;

    if (isSystemOverviewActive() && isDagBoundaryNode(draggedNode) && targetKey && targetKey !== SYSTEM_OVERVIEW_TARGET_KEY) {
      void changeTarget(targetKey);
      return;
    }

    if (!confirmDiscardEditorChanges()) {
      syncScene();
      return;
    }

    state.openNodeId = drag.nodeId;
    render();
    return;
  }

  storePositions();
  syncScene();
}

function handleStageWheel(event) {
  event.preventDefault();

  const stage = document.querySelector("#canvas-stage");

  if (!stage) {
    return;
  }

  const rect = stage.getBoundingClientRect();
  const pointX = event.clientX - rect.left;
  const pointY = event.clientY - rect.top;
  const normalizedDelta = event.deltaMode === 1
    ? event.deltaY * 16
    : event.deltaMode === 2
      ? event.deltaY * rect.height
      : event.deltaY;
  const zoomFactor = Math.exp(-normalizedDelta * 0.001);
  const nextScale = clamp(state.viewport.scale * zoomFactor, MIN_SCALE, MAX_SCALE);

  zoomToPoint({
    pointX,
    pointY,
    nextScale
  });
}

function zoomCanvas(multiplier) {
  const stage = document.querySelector("#canvas-stage");

  if (!stage) {
    return;
  }

  const rect = stage.getBoundingClientRect();
  const nextScale = clamp(state.viewport.scale * multiplier, MIN_SCALE, MAX_SCALE);

  zoomToPoint({
    pointX: rect.width / 2,
    pointY: rect.height / 2,
    nextScale
  });
}

function zoomToPoint({ pointX, pointY, nextScale }) {
  const worldX = (pointX - state.viewport.x) / state.viewport.scale;
  const worldY = (pointY - state.viewport.y) / state.viewport.scale;

  state.viewport.scale = nextScale;
  state.viewport.x = pointX - worldX * nextScale;
  state.viewport.y = pointY - worldY * nextScale;
  state.layoutMeta = {
    ...resolvePersistedLayoutMeta(),
    viewportAdjusted: true
  };

  scheduleViewportSync();
  persistViewportDebounced();
}

function fitViewportToNodes({ persist = false } = {}) {
  if (!state.payload) {
    return;
  }

  const stage = document.querySelector("#canvas-stage");

  if (!stage) {
    return;
  }

  const bounds = getNodeBounds();
  const rect = stage.getBoundingClientRect();
  const safeRect = getSafeStageViewport(rect);
  const width = Math.max(bounds.maxX - bounds.minX, NODE_WIDTH);
  const height = Math.max(bounds.maxY - bounds.minY, NODE_HEIGHT);
  const fitSettings = deriveFitSettings({
    payload: state.payload,
    sceneSize: state.sceneSize,
    width,
    height,
    safeRect
  });
  const availableWidth = Math.max(safeRect.width - fitSettings.padding * 2, NODE_WIDTH);
  const availableHeight = Math.max(safeRect.height - fitSettings.padding * 2, NODE_HEIGHT);

  state.viewport.scale = clamp(
    Math.min(availableWidth / width, availableHeight / height, fitSettings.scaleCap),
    MIN_SCALE,
    MAX_SCALE
  );
  state.viewport.x = safeRect.left + safeRect.width / 2 - (bounds.minX + width / 2) * state.viewport.scale;
  state.viewport.y = safeRect.top + safeRect.height / 2 - (bounds.minY + height / 2) * state.viewport.scale;
  state.needsInitialFit = false;

  syncScene();

  if (persist) {
    persistLayoutState();
  }
}

function getSafeStageViewport(stageRect) {
  const leftInset = FIT_SIDE_INSET;
  const rightInset = FIT_SIDE_INSET;
  const topInset = getBottomInsetWithinStage(stageRect, document.querySelector(".top-controls"));
  const bottomInset = Math.max(
    getTopInsetFromBottomWithinStage(stageRect, document.querySelector(".legend-panel")),
    getTopInsetFromBottomWithinStage(stageRect, document.querySelector(".zoom-controls")),
    getTopInsetFromBottomWithinStage(stageRect, document.querySelector(".error-toast"))
  );

  const safeLeft = stageRect.left + leftInset;
  const safeTop = stageRect.top + topInset;
  const safeRight = stageRect.right - rightInset;
  const safeBottom = stageRect.bottom - bottomInset;

  return {
    left: safeLeft - stageRect.left,
    top: safeTop - stageRect.top,
    width: Math.max(safeRight - safeLeft, NODE_WIDTH + 112),
    height: Math.max(safeBottom - safeTop, NODE_HEIGHT + 112)
  };
}

function getBottomInsetWithinStage(stageRect, element) {
  if (!element) {
    return FIT_VERTICAL_BUFFER;
  }

  const rect = element.getBoundingClientRect();

  if (!rect.width || !rect.height) {
    return FIT_VERTICAL_BUFFER;
  }

  return Math.max(FIT_VERTICAL_BUFFER, rect.bottom - stageRect.top + FIT_VERTICAL_BUFFER);
}

function getTopInsetFromBottomWithinStage(stageRect, element) {
  if (!element) {
    return FIT_VERTICAL_BUFFER;
  }

  const rect = element.getBoundingClientRect();

  if (!rect.width || !rect.height) {
    return FIT_VERTICAL_BUFFER;
  }

  return Math.max(FIT_VERTICAL_BUFFER, stageRect.bottom - rect.top + FIT_VERTICAL_BUFFER);
}

function syncScene() {
  sceneSyncFrame = null;
  syncViewportTransform();

  const scene = document.querySelector("#canvas-scene");

  if (!scene || !state.payload) {
    return;
  }

  if (isSystemOverviewActive()) {
    syncSystemOverviewScene();
  } else {
    syncNodePositions();
    syncNodeState();
    drawEdges();
  }
  syncZoomOnly();
}

function syncViewportTransform() {
  viewportSyncFrame = null;
  const scene = document.querySelector("#canvas-scene");

  if (!scene) {
    return;
  }

  scene.style.transform = `translate3d(${state.viewport.x}px, ${state.viewport.y}px, 0) scale(${state.viewport.scale})`;
  syncZoomOnly();
}

function scheduleSceneSync() {
  if (sceneSyncFrame !== null) {
    return;
  }

  sceneSyncFrame = window.requestAnimationFrame(syncScene);
}

function scheduleViewportSync() {
  if (viewportSyncFrame !== null) {
    return;
  }

  viewportSyncFrame = window.requestAnimationFrame(syncViewportTransform);
}

function syncSystemOverviewScene() {
  if (!state.payload) {
    return;
  }

  const model = buildSystemOverviewVisualModel(state.payload.nodes);

  document.querySelectorAll(".system-overview-entry").forEach((entryElement) => {
    const entryId = entryElement.dataset.entryId || "";
    const entry = model.entries.find((candidate) => candidate.id === entryId);

    if (entry) {
      entryElement.style.left = `${entry.x}px`;
      entryElement.style.top = `${entry.y}px`;
    }
  });

  document.querySelectorAll(".system-overview-stage-frame").forEach((frameElement) => {
    const stageKey = frameElement.dataset.stageKey || "";
    const stage = model.stages.find((candidate) => candidate.key === stageKey);

    if (stage) {
      frameElement.style.left = `${stage.bounds.minX}px`;
      frameElement.style.top = `${stage.bounds.minY}px`;
      frameElement.style.width = `${stage.bounds.width}px`;
      frameElement.style.height = `${stage.bounds.height}px`;
    }
  });

  document.querySelectorAll(".system-overview-stage-label").forEach((labelElement) => {
    const stageKey = labelElement.dataset.stageKey || "";
    const stage = model.stages.find((candidate) => candidate.key === stageKey);

    if (stage) {
      labelElement.style.left = `${stage.labelX}px`;
      labelElement.style.top = `${stage.labelY}px`;
    }
  });

  const connectorsLayer = document.querySelector(".system-overview-connectors");

  if (connectorsLayer) {
    connectorsLayer.innerHTML = `
      <defs>
        <marker id="overview-arrowhead" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto">
          <path d="M 0 0 L 10 5 L 0 10 z" fill="#3a3936"></path>
        </marker>
      </defs>
      ${model.connectors.map((path) => `<path class="system-overview-connector" d="${path}"></path>`).join("")}
    `;
  }
}

function syncNodePositions() {
  document.querySelectorAll(".dag-node").forEach((nodeElement) => {
    const nodeId = nodeElement.dataset.nodeId;
    const position = state.positions[nodeId];

    if (position) {
      nodeElement.style.left = `${position.x}px`;
      nodeElement.style.top = `${position.y}px`;
    }
  });
}

function syncNodeState() {
  const focus = getLineageFocus();

  document.querySelectorAll(".dag-node").forEach((nodeElement) => {
    const nodeId = nodeElement.dataset.nodeId;

    nodeElement.classList.toggle("is-active", nodeId === state.activeNodeId);
    nodeElement.classList.toggle("is-open", nodeId === state.openNodeId);
    nodeElement.classList.toggle("is-muted", Boolean(focus) && !focus.nodeIds.has(nodeId));
  });
}

function syncZoomOnly() {
  const zoomValue = document.querySelector("#zoom-value");

  if (zoomValue) {
    zoomValue.textContent = `${Math.round(state.viewport.scale * 100)}%`;
  }
}

function syncStatusOnly() {
  if (IS_STATIC_MODE) {
    return;
  }

  const meta = document.querySelector("#status-meta");
  const pill = document.querySelector("#status-pill");

  if (meta) {
    meta.textContent = renderHeaderRefreshMeta(state.refresh);
  }

  if (pill) {
    pill.className = `dag-status-label ${renderStatusClass(state.refresh)}`;
    pill.textContent = renderStatusLabel(state.refresh);
  }
}

function getCurrentTarget() {
  const currentKey = state.payload?.target?.key;
  return state.targets.find((target) => target.key === currentKey) || null;
}

function getTargetsForCategory(categoryLabel) {
  return state.targets
    .filter((target) => (target.categoryLabel || "Other") === categoryLabel)
    .slice()
    .sort((left, right) => left.label.localeCompare(right.label));
}

function isSystemOverviewActive() {
  return state.payload?.target?.key === SYSTEM_OVERVIEW_TARGET_KEY;
}

function drawEdges() {
  const edgesLayer = document.querySelector("#edges-layer");

  if (!edgesLayer || !state.payload) {
    return;
  }

  const focus = getLineageFocus();

  edgesLayer.innerHTML = `
    <defs>
      <marker id="arrowhead" markerWidth="9" markerHeight="9" refX="8" refY="4.5" orient="auto">
        <path d="M 0 0 L 9 4.5 L 0 9 z" fill="#2d2d2d"></path>
      </marker>
    </defs>
    ${state.payload.edges
      .map((edge) => {
        const path = buildEdgePath(edge);

        if (!path) {
          return "";
        }

        const edgeKey = createEdgeKey(edge.source, edge.target);
        const isRelated = Boolean(focus) && focus.edgeKeys.has(edgeKey);
        const isMuted = Boolean(focus) && !isRelated;
        return `<path class="graph-edge ${isRelated ? "is-related" : ""} ${isMuted ? "is-muted" : ""}" d="${path}"></path>`;
      })
      .join("")}
  `;
}

function buildEdgePath(edge) {
  const source = state.positions[edge.source];
  const target = state.positions[edge.target];

  if (!source || !target) {
    return "";
  }

  const sourceCenter = {
    x: source.x + NODE_WIDTH / 2,
    y: source.y + NODE_HEIGHT / 2
  };
  const targetCenter = {
    x: target.x + NODE_WIDTH / 2,
    y: target.y + NODE_HEIGHT / 2
  };
  const dx = targetCenter.x - sourceCenter.x;
  const dy = targetCenter.y - sourceCenter.y;

  let start;
  let end;

  if (Math.abs(dx) >= Math.abs(dy)) {
    start = dx >= 0 ? { x: source.x + NODE_WIDTH, y: sourceCenter.y } : { x: source.x, y: sourceCenter.y };
    end = dx >= 0 ? { x: target.x, y: targetCenter.y } : { x: target.x + NODE_WIDTH, y: targetCenter.y };
    return `M ${start.x} ${start.y} L ${end.x} ${end.y}`;
  }

  start = dy >= 0 ? { x: sourceCenter.x, y: source.y + NODE_HEIGHT } : { x: sourceCenter.x, y: source.y };
  end = dy >= 0 ? { x: targetCenter.x, y: target.y } : { x: targetCenter.x, y: target.y + NODE_HEIGHT };
  return `M ${start.x} ${start.y} L ${end.x} ${end.y}`;
}

function getLineageFocus() {
  if (!state.payload || !state.activeNodeId) {
    return null;
  }

  const nodeIds = new Set([state.activeNodeId]);
  const downstream = new Map();
  const upstream = new Map();

  state.payload.edges.forEach((edge) => {
    if (!downstream.has(edge.source)) {
      downstream.set(edge.source, []);
    }

    if (!upstream.has(edge.target)) {
      upstream.set(edge.target, []);
    }

    downstream.get(edge.source).push(edge.target);
    upstream.get(edge.target).push(edge.source);
  });

  walkConnectedNodes(state.activeNodeId, downstream, nodeIds);
  walkConnectedNodes(state.activeNodeId, upstream, nodeIds);

  const edgeKeys = new Set(
    state.payload.edges
      .filter((edge) => nodeIds.has(edge.source) && nodeIds.has(edge.target))
      .map((edge) => createEdgeKey(edge.source, edge.target))
  );

  return {
    nodeIds,
    edgeKeys
  };
}

function walkConnectedNodes(startNodeId, adjacency, collector) {
  const stack = [...(adjacency.get(startNodeId) || [])];

  while (stack.length) {
    const nodeId = stack.pop();

    if (collector.has(nodeId)) {
      continue;
    }

    collector.add(nodeId);

    for (const nextNodeId of adjacency.get(nodeId) || []) {
      if (!collector.has(nextNodeId)) {
        stack.push(nextNodeId);
      }
    }
  }
}

function createEdgeKey(sourceId, targetId) {
  return `${sourceId}|||${targetId}`;
}

function handleResize() {
  if (state.needsInitialFit && state.payload) {
    fitViewportToNodes({ persist: false });
    return;
  }

  syncScene();
}

function handleGlobalKeydown(event) {
  if (event.key === "Escape" && state.openNodeId) {
    closeModal();
    return;
  }

  if (event.key === "Escape" && state.launcherOpen) {
    closeLauncher();
    return;
  }

  if ((event.key === "=" || event.key === "+") && !state.openNodeId) {
    event.preventDefault();
    zoomCanvas(1.12);
    return;
  }

  if (event.key === "-" && !state.openNodeId) {
    event.preventDefault();
    zoomCanvas(1 / 1.12);
    return;
  }

  if (event.key.toLowerCase() === "f" && !state.openNodeId) {
    event.preventDefault();
    handleFitClick();
  }
}

function handleFitClick({ persist = false } = {}) {
  if (!state.payload) {
    return;
  }

  const layoutPlan = deriveLayoutPlan(state.payload.nodes, state.payload.edges);
  state.sceneSize = layoutPlan.sceneSize;
  state.positions = { ...layoutPlan.defaultPositions };
  state.layoutMeta = createLayoutMeta(layoutPlan, {
    manualAdjusted: false,
    viewportAdjusted: false
  });
  state.viewport = { ...DEFAULT_VIEWPORT };
  state.needsInitialFit = true;
  render();

  if (persist) {
    persistLayoutState();
  }
}

function closeModal() {
  if (!confirmDiscardEditorChanges()) {
    return;
  }

  state.openNodeId = null;
  state.modalTabNodeId = null;
  state.modalTab = null;
  state.editor = createEmptyEditorState();
  render();
}

function confirmDiscardEditorChanges() {
  if (!state.editor.dirty || state.editor.isSaving) {
    return !state.editor.isSaving;
  }

  const shouldDiscard = window.confirm("Discard your unsaved changes?");

  if (!shouldDiscard) {
    return false;
  }

  state.editor = createEmptyEditorState();
  return true;
}

function getOpenNode() {
  if (isSystemOverviewActive() || !state.openNodeId || !state.payload) {
    return null;
  }

  return state.payload.nodes.find((node) => node.id === state.openNodeId) || null;
}

function syncEditorStateWithOpenNode() {
  const openNode = getOpenNode();

  if (!openNode) {
    state.modalTabNodeId = null;
    state.modalTab = null;
    state.editor = createEmptyEditorState();
    return;
  }

  if (state.editor.nodeId !== openNode.id) {
    state.editor = createEditorStateForNode(openNode, {
      isEditing: false,
      focusField: null
    });
    return;
  }

  if (!state.editor.dirty && !state.editor.isSaving) {
    state.editor = createEditorStateForNode(openNode, {
      isEditing: state.editor.isEditing,
      focusField: state.editor.focusField
    });
  }
}

function createEditorStateForNode(node, { isEditing = false, focusField = null } = {}) {
  const draft = createEditorDraftFromNode(node);

  return {
    nodeId: node.id,
    isEditing,
    isSaving: false,
    dirty: false,
    focusField,
    error: null,
    initialDraft: cloneDraft(draft),
    draft
  };
}

function createEditorDraftFromNode(node) {
  return {
    description: node.description || "",
    grain: node.curated.grain || "",
    transformationStepsText: getEditableTransformationText(node),
    sql: node.sql || "",
    columns: node.columns.map((column) => ({
      name: column.name,
      description: column.description || "",
      dataType: column.dataType || "",
      mappingInstructions: column.mappingInstructions || "",
      isPrimaryKey: Boolean(column.isPrimaryKey)
    }))
  };
}

function cloneDraft(draft) {
  return JSON.parse(JSON.stringify(draft));
}

function getEditableTransformationText(node) {
  if (Array.isArray(node.curated.transformationSteps)) {
    return node.curated.transformationSteps.join("\n");
  }

  if (typeof node.curated.transformationSteps === "string") {
    return node.curated.transformationSteps.trimEnd();
  }

  return "";
}

function canEditNode(node) {
  return !IS_STATIC_MODE && Boolean(node) && Boolean(state.capabilities?.canEdit) && !isDagBoundaryNode(node);
}

function getEditorState(node) {
  if (!node || state.editor.nodeId !== node.id) {
    return createEditorStateForNode(node || { id: "", description: "", curated: {}, columns: [], sql: "" });
  }

  return state.editor;
}

function setEditorDirtyFlag() {
  if (!state.editor.initialDraft || !state.editor.draft) {
    state.editor.dirty = false;
    return;
  }

  state.editor.dirty = JSON.stringify(state.editor.initialDraft) !== JSON.stringify(state.editor.draft);
}

function chooseOpenNodeId({ payload, preferredNodeId }) {
  if (!payload || payload.target?.key === SYSTEM_OVERVIEW_TARGET_KEY || !preferredNodeId) {
    return null;
  }

  return payload.nodes.some((node) => node.id === preferredNodeId) ? preferredNodeId : null;
}

function chooseNodeId({ payload, preferredNodeId, allowFallback = true }) {
  if (!payload) {
    return null;
  }

  if (payload.target?.key === SYSTEM_OVERVIEW_TARGET_KEY) {
    return null;
  }

  if (preferredNodeId && payload.nodes.some((node) => node.id === preferredNodeId)) {
    return preferredNodeId;
  }

  if (!allowFallback) {
    return null;
  }

  if (
    payload.target?.defaultSelectedNodeId &&
    payload.nodes.some((node) => node.id === payload.target.defaultSelectedNodeId)
  ) {
    return payload.target.defaultSelectedNodeId;
  }

  return payload.nodes[payload.nodes.length - 1]?.id || payload.nodes[0]?.id || null;
}

function buildNodePositions(nodes, {
  currentPositions,
  storedPositions,
  defaultPositions,
  sceneSize = state.sceneSize
}) {
  const positions = {};

  nodes.forEach((node) => {
    positions[node.id] = clampPosition(
      currentPositions?.[node.id] || storedPositions?.[node.id] || defaultPositions?.[node.id] || { x: 120, y: 120 },
      sceneSize
    );
  });

  return positions;
}

function buildSystemOverviewEntryPositions({
  currentPositions,
  storedPositions,
  defaultPositions,
  sceneSize = state.sceneSize
}) {
  return Object.fromEntries(
    SYSTEM_OVERVIEW_ENTRY_LAYOUT.map((entry) => [
      entry.id,
      clampOverviewEntryPosition(
        currentPositions?.[entry.id] ||
          storedPositions?.[entry.id] ||
          defaultPositions?.[entry.id] ||
          { x: entry.x, y: entry.y },
        sceneSize
      )
    ])
  );
}

function deriveLayoutPlan(nodes, edges) {
  if (state.payload?.target?.key === SYSTEM_OVERVIEW_TARGET_KEY) {
    return deriveSystemOverviewLayoutPlan(nodes, edges);
  }

  const groupedNodes = orderModelGroups(groupNodesByDepth(nodes), edges);
  const seedNodes = orderSeedNodes(nodes.filter((node) => isSeedNode(node)), edges, groupedNodes);
  const sceneSize = deriveSceneSize({
    groupedNodes,
    seedNodes,
    totalNodeCount: nodes.length
  });
  const defaultPositions = deriveDefaultPositions({
    groupedNodes,
    seedNodes,
    sceneSize
  });

  return {
    groupedNodes,
    seedNodes,
    sceneSize,
    defaultPositions,
    signature: createLayoutSignature(nodes, edges)
  };
}

function deriveSystemOverviewLayoutPlan(nodes, edges) {
  const positions = {};
  nodes.forEach((node) => {
    positions[node.id] = { ...SYSTEM_OVERVIEW_HIDDEN_POSITION };
  });

  const model = buildSystemOverviewVisualModel(nodes);
  const bounds = model.stages.length
    ? {
        minX: Math.min(...model.stages.map((stage) => stage.bounds.minX)),
        minY: Math.min(...model.stages.map((stage) => stage.labelY)),
        maxX: Math.max(...model.stages.map((stage) => stage.bounds.minX + stage.bounds.width)),
        maxY: Math.max(...model.stages.map((stage) => stage.bounds.minY + stage.bounds.height))
      }
    : {
        minX: 0,
        minY: 0,
        maxX: SYSTEM_OVERVIEW_ENTRY_WIDTH,
        maxY: SYSTEM_OVERVIEW_ENTRY_HEIGHT
      };
  const sceneSize = {
    width: Math.max(DEFAULT_SCENE_SIZE.width, bounds.maxX + 220),
    height: Math.max(DEFAULT_SCENE_SIZE.height, bounds.maxY + 180),
    columnCount: 5,
    totalNodeCount: nodes.length,
    maxModelCount: Math.max(model.entries.length, 1)
  };

  return {
    groupedNodes: [],
    seedNodes: [],
    sceneSize,
    defaultPositions: positions,
    signature: `${createLayoutSignature(nodes, edges)}||system-overview-v${SYSTEM_OVERVIEW_LAYOUT_VERSION}`
  };
}

function placeOverviewGroup(positions, nodeByTargetKey, targetKeys, x, startY, rowGap) {
  targetKeys.forEach((targetKey, index) => {
    const node = nodeByTargetKey.get(targetKey);

    if (!node) {
      return;
    }

    positions[node.id] = {
      x,
      y: Math.round(startY + index * rowGap)
    };
  });
}

function computeBoundsFromPositions(positions) {
  const values = Object.values(positions);

  if (!values.length) {
    return { minX: 0, minY: 0, maxX: NODE_WIDTH, maxY: NODE_HEIGHT };
  }

  return values.reduce(
    (bounds, position) => ({
      minX: Math.min(bounds.minX, position.x),
      minY: Math.min(bounds.minY, position.y),
      maxX: Math.max(bounds.maxX, position.x),
      maxY: Math.max(bounds.maxY, position.y)
    }),
    {
      minX: Number.POSITIVE_INFINITY,
      minY: Number.POSITIVE_INFINITY,
      maxX: Number.NEGATIVE_INFINITY,
      maxY: Number.NEGATIVE_INFINITY
    }
  );
}

function deriveSceneSize({ groupedNodes, seedNodes, totalNodeCount }) {
  const columnCount = Math.max(groupedNodes.length, 1);
  const maxModelCount = Math.max(
    1,
    ...groupedNodes.map((group) => group.modelNodes.length || 0)
  );
  const columnGap = deriveColumnGap({
    columnCount,
    totalNodeCount,
    maxModelCount
  });
  const width = Math.max(
    DEFAULT_SCENE_SIZE.width,
    SCENE_MARGIN_LEFT + (columnCount - 1) * columnGap + NODE_WIDTH + SCENE_MARGIN_RIGHT
  );
  const availableSeedWidth = Math.max(
    NODE_WIDTH,
    width - SCENE_MARGIN_LEFT - SCENE_MARGIN_RIGHT
  );
  const seedColumns = Math.max(
    1,
    Math.min(
      seedNodes.length || 1,
      Math.floor((availableSeedWidth + SEED_HORIZONTAL_GAP) / (NODE_WIDTH + SEED_HORIZONTAL_GAP))
    )
  );
  const seedRows = seedNodes.length ? Math.ceil(seedNodes.length / seedColumns) : 0;
  const seedRowGap = seedRows > 1
    ? clamp(
        (DEFAULT_SCENE_SIZE.height - SCENE_MARGIN_TOP - SCENE_MARGIN_BOTTOM - SEED_BAND_PADDING - NODE_HEIGHT) / (seedRows - 1),
        MIN_SEED_VERTICAL_GAP,
        MAX_SEED_VERTICAL_GAP
      )
    : 0;
  const topBandHeight = seedRows
    ? SCENE_MARGIN_TOP + NODE_HEIGHT + Math.max(0, seedRows - 1) * seedRowGap + SEED_BAND_PADDING
    : SCENE_MARGIN_TOP + 24;
  const bandGap = clamp(
    152 - Math.max(0, Math.min(totalNodeCount, 28) - 6) * 2,
    MIN_BAND_GAP,
    MAX_BAND_GAP
  );
  const modelBandHeight = deriveModelBandHeight(maxModelCount);
  const height = Math.max(
    DEFAULT_SCENE_SIZE.height,
    topBandHeight + bandGap + modelBandHeight + SCENE_MARGIN_BOTTOM
  );

  return {
    width,
    height,
    columnGap,
    seedColumns,
    seedRows,
    seedRowGap,
    topBandHeight,
    modelBandTop: topBandHeight + bandGap,
    modelBandHeight,
    columnCount,
    totalNodeCount,
    maxModelCount
  };
}

function deriveDefaultPositions({ groupedNodes, seedNodes, sceneSize = state.sceneSize }) {
  const positions = {};

  if (seedNodes.length) {
    const seedColumns = Math.max(1, sceneSize.seedColumns || 1);
    const rowCount = Math.max(1, sceneSize.seedRows || Math.ceil(seedNodes.length / seedColumns));

    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      const rowStart = rowIndex * seedColumns;
      const rowNodes = seedNodes.slice(rowStart, rowStart + seedColumns);
      const rowWidth = rowNodes.length * NODE_WIDTH + Math.max(0, rowNodes.length - 1) * SEED_HORIZONTAL_GAP;
      const startX = SCENE_MARGIN_LEFT + Math.max(0, (sceneSize.width - SCENE_MARGIN_LEFT - SCENE_MARGIN_RIGHT - rowWidth) / 2);
      const y = SCENE_MARGIN_TOP + rowIndex * (sceneSize.seedRowGap || 0);

      rowNodes.forEach((node, columnIndex) => {
        positions[node.id] = {
          x: startX + columnIndex * (NODE_WIDTH + SEED_HORIZONTAL_GAP),
          y
        };
      });
    }
  }

  groupedNodes.forEach((group, depthIndex) => {
    const x = SCENE_MARGIN_LEFT + depthIndex * (sceneSize.columnGap || MIN_COLUMN_GAP);

    if (!group.modelNodes.length) {
      return;
    }

    const nodeCount = group.modelNodes.length;
    const modelGap = nodeCount > 1
      ? clamp(
          (sceneSize.modelBandHeight - NODE_HEIGHT) / (nodeCount - 1),
          MIN_MODEL_VERTICAL_GAP,
          MAX_MODEL_VERTICAL_GAP
        )
      : 0;
    const usedHeight = NODE_HEIGHT + Math.max(0, nodeCount - 1) * modelGap;
    const startY = sceneSize.modelBandTop + Math.max(0, (sceneSize.modelBandHeight - usedHeight) / 2);

    group.modelNodes.forEach((node, rowIndex) => {
      positions[node.id] = {
        x,
        y: startY + rowIndex * modelGap
      };
    });
  });

  return positions;
}

function getNodeBounds() {
  if (isSystemOverviewActive() && state.payload) {
    const model = buildSystemOverviewVisualModel(state.payload.nodes);

    if (!model.entries.length || !model.stages.length) {
      return {
        minX: 0,
        minY: 0,
        maxX: SYSTEM_OVERVIEW_ENTRY_WIDTH,
        maxY: SYSTEM_OVERVIEW_ENTRY_HEIGHT
      };
    }

    return {
      minX: Math.min(...model.stages.map((stage) => stage.bounds.minX)),
      minY: Math.min(...model.stages.map((stage) => stage.labelY)),
      maxX: Math.max(...model.stages.map((stage) => stage.bounds.minX + stage.bounds.width)),
      maxY: Math.max(...model.stages.map((stage) => stage.bounds.minY + stage.bounds.height))
    };
  }

  const values = Object.values(state.positions);

  if (!values.length) {
    return {
      minX: 0,
      minY: 0,
      maxX: NODE_WIDTH,
      maxY: NODE_HEIGHT
    };
  }

  return values.reduce(
    (bounds, position) => {
      return {
        minX: Math.min(bounds.minX, position.x),
        minY: Math.min(bounds.minY, position.y),
        maxX: Math.max(bounds.maxX, position.x + NODE_WIDTH),
        maxY: Math.max(bounds.maxY, position.y + NODE_HEIGHT)
      };
    },
    {
      minX: Number.POSITIVE_INFINITY,
      minY: Number.POSITIVE_INFINITY,
      maxX: Number.NEGATIVE_INFINITY,
      maxY: Number.NEGATIVE_INFINITY
    }
  );
}

function groupNodesByDepth(nodes) {
  const groups = new Map();

  nodes.forEach((node) => {
    if (!groups.has(node.depth)) {
      groups.set(node.depth, []);
    }

    groups.get(node.depth).push(node);
  });

  return Array.from(groups.entries())
    .sort((left, right) => left[0] - right[0])
    .map(([depth, groupedNodes]) => {
      const sortedNodes = groupedNodes.slice().sort((left, right) => left.runOrder - right.runOrder);

      return {
        depth,
        nodes: sortedNodes,
        seedNodes: sortedNodes.filter((node) => isSeedNode(node)),
        modelNodes: sortedNodes.filter((node) => !isSeedNode(node))
      };
    });
}

function orderModelGroups(groupedNodes, edges) {
  const groups = groupedNodes.map((group) => ({
    ...group,
    nodes: [...group.nodes],
    seedNodes: [...group.seedNodes],
    modelNodes: [...group.modelNodes]
  }));
  const adjacency = buildEdgeAdjacency(edges);

  if (groups.length <= 1) {
    return groups;
  }

  for (let sweep = 0; sweep < 3; sweep += 1) {
    let orderLookup = createModelOrderLookup(groups);

    for (let groupIndex = 1; groupIndex < groups.length; groupIndex += 1) {
      groups[groupIndex].modelNodes = sortNodesByNeighborOrder(groups[groupIndex].modelNodes, adjacency.upstream, orderLookup);
      groups[groupIndex].nodes = [...groups[groupIndex].seedNodes, ...groups[groupIndex].modelNodes];
      orderLookup = createModelOrderLookup(groups);
    }

    for (let groupIndex = groups.length - 2; groupIndex >= 0; groupIndex -= 1) {
      groups[groupIndex].modelNodes = sortNodesByNeighborOrder(groups[groupIndex].modelNodes, adjacency.downstream, orderLookup);
      groups[groupIndex].nodes = [...groups[groupIndex].seedNodes, ...groups[groupIndex].modelNodes];
      orderLookup = createModelOrderLookup(groups);
    }
  }

  return groups;
}

function orderSeedNodes(seedNodes, edges, groupedNodes) {
  const adjacency = buildEdgeAdjacency(edges);
  const orderLookup = createModelOrderLookup(groupedNodes);
  const depthLookup = createDepthLookup(groupedNodes);

  return seedNodes.slice().sort((left, right) => {
    const leftAnchor = resolveSeedAnchor(left, adjacency.downstream, orderLookup, depthLookup);
    const rightAnchor = resolveSeedAnchor(right, adjacency.downstream, orderLookup, depthLookup);

    if (leftAnchor.depth !== rightAnchor.depth) {
      return leftAnchor.depth - rightAnchor.depth;
    }

    if (leftAnchor.order !== rightAnchor.order) {
      return leftAnchor.order - rightAnchor.order;
    }

    return left.runOrder - right.runOrder || left.name.localeCompare(right.name);
  });
}

function deriveColumnGap({ columnCount, totalNodeCount, maxModelCount }) {
  const complexityPenalty = Math.max(0, totalNodeCount - 8) * 2 + Math.max(0, maxModelCount - 3) * 8;
  return clamp(MAX_COLUMN_GAP - complexityPenalty - Math.max(0, columnCount - 2) * 12, MIN_COLUMN_GAP, MAX_COLUMN_GAP);
}

function deriveModelBandHeight(maxModelCount) {
  if (maxModelCount <= 1) {
    return NODE_HEIGHT + 120;
  }

  const targetGap = maxModelCount <= 3
    ? MAX_MODEL_VERTICAL_GAP
    : maxModelCount <= 6
      ? 152
      : maxModelCount <= 10
        ? 136
        : MIN_MODEL_VERTICAL_GAP;

  return NODE_HEIGHT + (maxModelCount - 1) * targetGap + 88;
}

function buildEdgeAdjacency(edges) {
  const upstream = new Map();
  const downstream = new Map();

  edges.forEach((edge) => {
    if (!downstream.has(edge.source)) {
      downstream.set(edge.source, []);
    }

    if (!upstream.has(edge.target)) {
      upstream.set(edge.target, []);
    }

    downstream.get(edge.source).push(edge.target);
    upstream.get(edge.target).push(edge.source);
  });

  return {
    upstream,
    downstream
  };
}

function createModelOrderLookup(groupedNodes) {
  const lookup = new Map();

  groupedNodes.forEach((group) => {
    group.modelNodes.forEach((node, index) => {
      lookup.set(node.id, index);
    });
  });

  return lookup;
}

function createDepthLookup(groupedNodes) {
  const lookup = new Map();

  groupedNodes.forEach((group, index) => {
    group.modelNodes.forEach((node) => {
      lookup.set(node.id, index);
    });
  });

  return lookup;
}

function sortNodesByNeighborOrder(nodes, relationMap, orderLookup) {
  return nodes.slice().sort((left, right) => {
    const leftValue = computeNeighborBarycenter(left, relationMap, orderLookup);
    const rightValue = computeNeighborBarycenter(right, relationMap, orderLookup);
    const leftFinite = Number.isFinite(leftValue);
    const rightFinite = Number.isFinite(rightValue);

    if (leftFinite && rightFinite && leftValue !== rightValue) {
      return leftValue - rightValue;
    }

    if (leftFinite !== rightFinite) {
      return leftFinite ? -1 : 1;
    }

    return left.runOrder - right.runOrder || left.name.localeCompare(right.name);
  });
}

function computeNeighborBarycenter(node, relationMap, orderLookup) {
  const neighbors = (relationMap.get(node.id) || [])
    .map((neighborId) => orderLookup.get(neighborId))
    .filter((value) => Number.isFinite(value));

  if (!neighbors.length) {
    return Number.NaN;
  }

  return neighbors.reduce((sum, value) => sum + value, 0) / neighbors.length;
}

function resolveSeedAnchor(node, relationMap, orderLookup, depthLookup) {
  const neighbors = (relationMap.get(node.id) || []).filter((neighborId) => depthLookup.has(neighborId));

  if (!neighbors.length) {
    return {
      depth: Number.POSITIVE_INFINITY,
      order: Number.POSITIVE_INFINITY
    };
  }

  const depth = neighbors.reduce((sum, neighborId) => sum + depthLookup.get(neighborId), 0) / neighbors.length;
  const order = neighbors.reduce((sum, neighborId) => sum + (orderLookup.get(neighborId) || 0), 0) / neighbors.length;

  return { depth, order };
}

function clampPosition(position, sceneSize = state.sceneSize) {
  const inset = state.payload?.target?.key === SYSTEM_OVERVIEW_TARGET_KEY ? 0 : 40;

  return {
    x: clamp(position.x, inset, sceneSize.width - NODE_WIDTH - inset),
    y: clamp(position.y, inset, sceneSize.height - NODE_HEIGHT - inset)
  };
}

function clampOverviewEntryPosition(position, sceneSize = state.sceneSize) {
  return {
    x: clamp(position.x, 0, sceneSize.width - SYSTEM_OVERVIEW_ENTRY_WIDTH),
    y: clamp(position.y, 0, sceneSize.height - SYSTEM_OVERVIEW_ENTRY_HEIGHT)
  };
}

function clampViewport(viewport) {
  return {
    x: Number.isFinite(viewport?.x) ? viewport.x : DEFAULT_VIEWPORT.x,
    y: Number.isFinite(viewport?.y) ? viewport.y : DEFAULT_VIEWPORT.y,
    scale: clamp(Number.isFinite(viewport?.scale) ? viewport.scale : DEFAULT_VIEWPORT.scale, MIN_SCALE, MAX_SCALE)
  };
}

function isViewportLike(value) {
  return value && Number.isFinite(value.x) && Number.isFinite(value.y) && Number.isFinite(value.scale);
}

function deriveFitSettings({ payload, sceneSize, width, height, safeRect }) {
  if (payload?.target?.key === SYSTEM_OVERVIEW_TARGET_KEY) {
    return {
      scaleCap: 0.68,
      padding: 18
    };
  }

  const nodeCount = payload?.nodes?.length || 0;
  const densityRatio = Math.max(
    width / Math.max(safeRect.width, NODE_WIDTH),
    height / Math.max(safeRect.height, NODE_HEIGHT)
  );
  let scaleCap = 1.14;
  let padding = 40;

  if (nodeCount <= 6 && densityRatio <= 0.7) {
    scaleCap = 1.52;
    padding = 56;
  } else if (nodeCount <= 12 && densityRatio <= 0.92) {
    scaleCap = 1.34;
    padding = 48;
  } else if (nodeCount <= 22) {
    scaleCap = 1.16;
    padding = 40;
  } else if (nodeCount <= 34) {
    scaleCap = 1.0;
    padding = 34;
  } else {
    scaleCap = 0.86;
    padding = 28;
  }

  if (sceneSize?.columnCount >= 6 || sceneSize?.maxModelCount >= 8) {
    scaleCap = Math.min(scaleCap, 0.98);
    padding = Math.min(padding, 34);
  }

  return { scaleCap, padding };
}

function storeViewport() {
  if (!state.payload) {
    return;
  }

  persistLayoutState({
    manualAdjusted: state.layoutMeta?.manualAdjusted || false,
    viewportAdjusted: true
  });
}

function storePositions() {
  if (!state.payload) {
    return;
  }

  persistLayoutState({
    manualAdjusted: true,
    viewportAdjusted: state.layoutMeta?.viewportAdjusted || false
  });
}

function getViewportStorageKey(targetKey) {
  return `tuva-dag:v${LAYOUT_STORAGE_VERSION}:${targetKey}:viewport`;
}

function getPositionsStorageKey(targetKey) {
  return `tuva-dag:v${LAYOUT_STORAGE_VERSION}:${targetKey}:positions`;
}

function getLayoutMetaKey(targetKey) {
  return `tuva-dag:v${LAYOUT_STORAGE_VERSION}:${targetKey}:layout-meta`;
}

function createLayoutSignature(nodes, edges) {
  const nodeSignature = nodes
    .map((node) => `${node.id}:${node.depth}:${node.nodeType}`)
    .sort()
    .join("|");
  const edgeSignature = edges
    .map((edge) => `${edge.source}>${edge.target}`)
    .sort()
    .join("|");

  return `${nodeSignature}||${edgeSignature}`;
}

function createLayoutMeta(layoutPlan, {
  manualAdjusted = false,
  viewportAdjusted = false
} = {}) {
  return {
    version: LAYOUT_STORAGE_VERSION,
    signature: layoutPlan.signature,
    manualAdjusted,
    viewportAdjusted
  };
}

function readLayoutMeta(targetKey) {
  const value = readStorageJson(getLayoutMetaKey(targetKey));

  if (!isCurrentLayoutMeta(value)) {
    return null;
  }

  return value;
}

function isCurrentLayoutMeta(value) {
  return Boolean(
    value &&
      value.version === LAYOUT_STORAGE_VERSION &&
      typeof value.signature === "string"
  );
}

function isMatchingLayoutMeta(value, signature) {
  return Boolean(isCurrentLayoutMeta(value) && value.signature === signature);
}

function resolvePersistedLayoutMeta({
  manualAdjusted = state.layoutMeta?.manualAdjusted || false,
  viewportAdjusted = state.layoutMeta?.viewportAdjusted || false
} = {}) {
  if (!state.payload) {
    return null;
  }

  return {
    version: LAYOUT_STORAGE_VERSION,
    signature: state.layoutMeta?.signature || createLayoutSignature(state.payload.nodes, state.payload.edges),
    manualAdjusted,
    viewportAdjusted
  };
}

function persistLayoutState(options = {}) {
  if (!state.payload) {
    return;
  }

  const targetKey = state.payload.target?.key || "appointment";
  state.layoutMeta = resolvePersistedLayoutMeta(options);
  writeStorageJson(getViewportStorageKey(targetKey), state.viewport);
  writeStorageJson(getPositionsStorageKey(targetKey), state.positions);
  writeStorageJson(getLayoutMetaKey(targetKey), state.layoutMeta);
}

function clearStoredLayoutState(targetKey) {
  try {
    window.localStorage.removeItem(getViewportStorageKey(targetKey));
    window.localStorage.removeItem(getPositionsStorageKey(targetKey));
    window.localStorage.removeItem(getLayoutMetaKey(targetKey));
  } catch (error) {
    return;
  }
}

function readStorageJson(key) {
  try {
    const value = window.localStorage.getItem(key);
    return value ? JSON.parse(value) : null;
  } catch (error) {
    return null;
  }
}

function writeStorageJson(key, value) {
  try {
    window.localStorage.setItem(key, JSON.stringify(value));
  } catch (error) {
    return null;
  }

  return value;
}

function createDefaultRefreshState() {
  return {
    status: "idle",
    payloadVersion: 0,
    activeTargetKey: "appointment",
    activeTrigger: "startup",
    lastAttemptAt: null,
    lastSuccessAt: null,
    lastError: null,
    hasPayload: false
  };
}

function getInitialTargetKey() {
  return initialSearchParams.get("target") || SYSTEM_OVERVIEW_TARGET_KEY;
}

function updateTargetQuery(targetKey) {
  const url = new URL(window.location.href);
  url.searchParams.set("target", targetKey);
  window.history.replaceState({}, "", url);
}

function resolveNodeColor(nodeType) {
  return nodeTypeColors[nodeType] || nodeTypeColors.intermediate;
}

function renderStatusClass(refresh) {
  if (refresh.status === "failed") {
    return "is-error";
  }

  if (refresh.status === "refreshing") {
    return "is-running";
  }

  return "is-ready";
}

function renderStatusLabel(refresh) {
  if (isStaticRefresh(refresh)) {
    return "Snapshot";
  }

  if (refresh.status === "failed") {
    return "Refresh failed";
  }

  if (refresh.status === "refreshing") {
    return "Refreshing";
  }

  return "Ready";
}

function renderRefreshSummary(refresh) {
  if (isStaticRefresh(refresh)) {
    if (refresh.lastSuccessAt) {
      return `Static docs snapshot exported ${formatTimestampCompact(refresh.lastSuccessAt)}.`;
    }

    return "Static docs snapshot.";
  }

  if (refresh.status === "refreshing") {
    return "Parsing dbt and rebuilding the DAG.";
  }

  if (refresh.status === "failed") {
    return refresh.lastError?.message || "Refresh failed. The last good DAG is still visible.";
  }

  if (refresh.lastSuccessAt) {
    return `Updated ${formatTimestampCompact(refresh.lastSuccessAt)}.`;
  }

  return "No successful refresh has completed yet.";
}

function renderHeaderRefreshMeta(refresh) {
  if (isStaticRefresh(refresh)) {
    if (refresh.lastSuccessAt) {
      return `Snapshot ${formatTimestampCompact(refresh.lastSuccessAt)}`;
    }

    return "Static snapshot";
  }

  if (refresh.status === "refreshing") {
    return "Updating DAG...";
  }

  if (refresh.status === "failed") {
    return "Last refresh failed";
  }

  if (refresh.lastSuccessAt) {
    return `Updated ${formatTimestampCompact(refresh.lastSuccessAt)}`;
  }

  return "Not yet updated";
}

function renderStatusPageTitle(refresh) {
  if (isStaticRefresh(refresh)) {
    return "DAG Snapshot";
  }

  return refresh.status === "refreshing" ? "Refreshing DAG" : "Waiting for DAG data";
}

function isStaticRefresh(refresh) {
  return IS_STATIC_MODE || refresh?.mode === "static";
}

function formatCuratedPrimaryKey(primaryKey) {
  if (!primaryKey) {
    return "";
  }

  if (Array.isArray(primaryKey)) {
    return primaryKey.join(", ");
  }

  return String(primaryKey);
}

function formatTimestamp(value) {
  if (!value) {
    return "Not yet available";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString([], {
    dateStyle: "medium",
    timeStyle: "short"
  });
}

function formatTimestampCompact(value) {
  if (!value) {
    return "Not yet available";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
  });
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttribute(value) {
  return escapeHtml(value);
}

function debounce(fn, delayMs) {
  let timeoutId = null;

  return (...args) => {
    window.clearTimeout(timeoutId);
    timeoutId = window.setTimeout(() => fn(...args), delayMs);
  };
}
