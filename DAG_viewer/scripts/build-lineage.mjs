import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { parse as parseYaml } from "yaml";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const dagRoot = path.resolve(scriptDir, "..");
const repoRoot = process.env.DAG_REPO_ROOT
  ? path.resolve(process.env.DAG_REPO_ROOT)
  : path.resolve(dagRoot, "..");
const cacheRoot = process.env.DAG_CACHE_DIR
  ? path.resolve(process.env.DAG_CACHE_DIR)
  : path.join(dagRoot, "data");
const manifestPath = process.env.DAG_MANIFEST_PATH
  ? path.resolve(process.env.DAG_MANIFEST_PATH)
  : path.join(repoRoot, "integration_tests", "target", "manifest.json");

export const DEFAULT_TARGET_KEY = "appointment";
export const SYSTEM_OVERVIEW_TARGET_KEY = "system_overview";

const packageRoots = {
  integration_tests: path.join(repoRoot, "integration_tests"),
  the_tuva_project: repoRoot
};

const fixedClaimsTargets = [
  {
    key: "service_categories",
    label: "Service Categories",
    kind: "claims_preprocessing",
    categoryKey: "claims_preprocessing",
    categoryLabel: "Claims Preprocessing",
    title: "Service Categories DAG",
    subtitle:
      "Claims preprocessing models that classify normalized medical claims into service categories before downstream encounters and claims-enrollment logic.",
    folderLabel: "claims_preprocessing/service_category",
    recurseWhenCollapsed: true,
    collapsedNodeType: "intermediate",
    matchesPath: (modelPath) => modelPath.startsWith("models/claims_preprocessing/service_category/"),
    selectRootNodeIds: (nodes) => nodes.filter((node) => node.original_file_path.includes("/final/")).map((node) => node.unique_id)
  },
  {
    key: "encounters",
    label: "Encounters",
    kind: "claims_preprocessing",
    categoryKey: "claims_preprocessing",
    categoryLabel: "Claims Preprocessing",
    title: "Encounters DAG",
    subtitle:
      "Claims preprocessing models that create encounter-grain outputs and the crosswalk/orphaned-claim assets consumed downstream in core medical claims.",
    folderLabel: "claims_preprocessing/encounters",
    recurseWhenCollapsed: true,
    collapsedNodeType: "intermediate",
    matchesPath: (modelPath) => modelPath.startsWith("models/claims_preprocessing/encounters/"),
    selectRootNodeIds: (nodes) => {
      const preferredNames = new Set(["encounters__combined_claim_line_crosswalk", "encounters__orphaned_claims"]);

      return nodes
        .filter((node) => node.original_file_path.includes("/final/") || preferredNames.has(node.name))
        .map((node) => node.unique_id);
    }
  },
  {
    key: "claims_enrollment",
    label: "Claims Enrollment",
    kind: "claims_preprocessing",
    categoryKey: "claims_preprocessing",
    categoryLabel: "Claims Preprocessing",
    title: "Claims Enrollment DAG",
    subtitle:
      "Claims preprocessing models that derive member-month context and enrollment flags used to annotate claims data before core outputs.",
    folderLabel: "claims_preprocessing/claims_enrollment",
    recurseWhenCollapsed: true,
    collapsedNodeType: "intermediate",
    matchesPath: (modelPath) => modelPath.startsWith("models/claims_preprocessing/claims_enrollment/"),
    selectRootNodeIds: (nodes) => {
      const preferredNames = new Set([
        "claims_enrollment__flag_claims_with_enrollment",
        "claims_enrollment__flag_rx_claims_with_enrollment",
        "claims_enrollment__member_months"
      ]);

      return nodes.filter((node) => preferredNames.has(node.name)).map((node) => node.unique_id);
    }
  }
];

const OVERVIEW_CATEGORY_ORDER = {
  input_layer: 0,
  claims_normalization: 1,
  claims_preprocessing: 2,
  core: 3,
  data_marts: 4
};

const labelOverrides = {
  appointment: "Appointment",
  ahrq_measures: "AHRQ Measures",
  ccsr: "CCSR",
  claims_enrollment: "Claims Enrollment",
  clinical_concept_library: "Clinical Concept Library",
  cms_hcc: "CMS HCC",
  ed_classification: "ED Classification",
  financial_pmpm: "Financial PMPM",
  fhir_preprocessing: "FHIR Preprocessing",
  hcc_recapture: "HCC Recapture",
  hcc_suspecting: "HCC Suspecting",
  medical_claim: "Medical Claim",
  member_months: "Member Months",
  person_id_crosswalk: "Person ID Crosswalk",
  provider_attribution: "Provider Attribution",
  quality_measures: "Quality Measures",
  readmissions: "Readmissions",
  semantic_layer: "Semantic Layer",
  service_categories: "Service Categories"
};

export async function getTargetConfig(targetKey = DEFAULT_TARGET_KEY) {
  const manifest = await loadManifest();
  const catalog = discoverTargetCatalog(manifest);
  return getTargetConfigFromCatalog(catalog, targetKey);
}

export async function listTargetConfigs() {
  const manifest = await loadManifest();
  const catalog = discoverTargetCatalog(manifest);

  return catalog.targets.map((target) => ({
    key: target.key,
    label: target.label,
    title: target.title,
    categoryKey: target.categoryKey,
    categoryLabel: target.categoryLabel,
    kind: target.kind,
    rootCount: target.rootNodeIds.length,
    defaultSelectedNodeId: target.defaultSelectedNodeId
  }));
}

export function getOutputPathForTarget(targetKey = DEFAULT_TARGET_KEY) {
  return path.join(cacheRoot, `${targetKey}-lineage.json`);
}

export async function buildLineagePayload({ targetKey = DEFAULT_TARGET_KEY } = {}) {
  const manifest = await loadManifest();
  const catalog = discoverTargetCatalog(manifest);
  const target = getTargetConfigFromCatalog(catalog, targetKey);

  if (target.key === SYSTEM_OVERVIEW_TARGET_KEY) {
    return buildSystemOverviewPayload({ manifest, catalog, target });
  }

  const nodeMap = manifest.nodes || {};
  const yamlCache = new Map();
  const graph = collectVisibleGraph({ manifest, catalog, target });
  const nodesById = new Map();
  const depthById = computeDisplayDepths(graph.edges);
  const orderedDisplayNodeIds = Array.from(graph.displayNodes.keys()).sort((leftId, rightId) => {
    const leftDepth = depthById.get(leftId) || 0;
    const rightDepth = depthById.get(rightId) || 0;

    if (leftDepth !== rightDepth) {
      return leftDepth - rightDepth;
    }

    const leftNode = graph.displayNodes.get(leftId);
    const rightNode = graph.displayNodes.get(rightId);
    const leftName = leftNode.kind === "collapsed" ? leftNode.target.label : nodeMap[leftNode.actualNodeId]?.name || leftId;
    const rightName =
      rightNode.kind === "collapsed" ? rightNode.target.label : nodeMap[rightNode.actualNodeId]?.name || rightId;

    return leftName.localeCompare(rightName);
  });
  const orderedModelNodeIds = orderedDisplayNodeIds.filter((displayNodeId) => {
    const displayNode = graph.displayNodes.get(displayNodeId);

    return displayNode.kind === "actual" && nodeMap[displayNode.actualNodeId]?.resource_type === "model";
  });
  const modelNodes = [];

  for (const displayNodeId of orderedModelNodeIds) {
    const displayNode = graph.displayNodes.get(displayNodeId);
    const manifestNode = nodeMap[displayNode.actualNodeId];
    const yamlEntry = await loadYamlEntry(manifestNode, yamlCache);
    const columns = buildColumns({
      manifestNode,
      yamlEntry,
      priorNodes: modelNodes
    });
    const baseNodeType = resolveBaseNodeType({
      manifestNode,
      nodeId: displayNode.actualNodeId,
      yamlEntry
    });

    modelNodes.push({
      id: displayNodeId,
      name: manifestNode.name,
      resourceType: manifestNode.resource_type,
      sourceStyle: isSourceDocumentationNode({
        manifestNode,
        nodeId: displayNode.actualNodeId
      }),
      layer: classifyLayer(manifestNode),
      depth: depthById.get(displayNodeId) || 0,
      folderLabel: deriveFolderLabel(manifestNode),
      description: cleanText(firstNonEmpty(yamlEntry?.description, manifestNode.description)),
      materialized: manifestNode.config?.materialized || manifestNode.resource_type || "model",
      technical: {
        alias: manifestNode.alias || manifestNode.name,
        schemaName: manifestNode.schema || null,
        packageName: manifestNode.package_name,
        tags: manifestNode.tags || [],
        primaryKeyColumns: columns.filter((column) => column.isPrimaryKey).map((column) => column.name)
      },
      paths: {
        sql: loadSqlPath(manifestNode),
        yaml: loadYamlPath(manifestNode),
        yamlEntryName: loadYamlEntryName(manifestNode),
        yamlCollectionKey: loadYamlCollectionKey(manifestNode),
        manifestNodeId: displayNode.actualNodeId,
        targetKey: target.key
      },
      mainDependencies: [],
      supportingDependencies: [],
      curated: extractCuratedMetadata(yamlEntry),
      baseNodeType,
      nodeType: resolveDisplayNodeType({
        baseNodeType,
        target,
        nodeId: displayNode.actualNodeId,
        resourceType: manifestNode.resource_type
      }),
      dagBoundary: null,
      seedViewer: null,
      sql: await loadSql(manifestNode),
      columns
    });
  }

  const seedNodes = [];

  for (const displayNodeId of orderedDisplayNodeIds) {
    const displayNode = graph.displayNodes.get(displayNodeId);

    if (displayNode.kind !== "actual") {
      continue;
    }

    const manifestNode = nodeMap[displayNode.actualNodeId];

    if (!manifestNode || manifestNode.resource_type !== "seed") {
      continue;
    }

    const yamlEntry = await loadYamlEntry(manifestNode, yamlCache);
    const columns = buildColumns({
      manifestNode,
      yamlEntry,
      priorNodes: []
    });
    const baseNodeType = resolveBaseNodeType({
      manifestNode,
      nodeId: displayNode.actualNodeId,
      yamlEntry
    });

    seedNodes.push({
      id: displayNodeId,
      name: manifestNode.name,
      resourceType: manifestNode.resource_type,
      sourceStyle: false,
      layer: classifyLayer(manifestNode),
      depth: depthById.get(displayNodeId) || 0,
      folderLabel: deriveFolderLabel(manifestNode),
      description: cleanText(firstNonEmpty(yamlEntry?.description, manifestNode.description)),
      materialized: manifestNode.resource_type || "seed",
      technical: {
        alias: manifestNode.alias || manifestNode.name,
        schemaName: manifestNode.schema || null,
        packageName: manifestNode.package_name,
        tags: manifestNode.tags || [],
        primaryKeyColumns: columns.filter((column) => column.isPrimaryKey).map((column) => column.name)
      },
      paths: {
        sql: loadSqlPath(manifestNode),
        yaml: loadYamlPath(manifestNode),
        yamlEntryName: loadYamlEntryName(manifestNode),
        yamlCollectionKey: loadYamlCollectionKey(manifestNode),
        manifestNodeId: displayNode.actualNodeId,
        targetKey: target.key
      },
      mainDependencies: [],
      supportingDependencies: [],
      curated: extractCuratedMetadata(yamlEntry),
      baseNodeType,
      nodeType: resolveDisplayNodeType({
        baseNodeType,
        target,
        nodeId: displayNode.actualNodeId,
        resourceType: manifestNode.resource_type
      }),
      dagBoundary: null,
      seedViewer: buildSeedViewer(manifestNode),
      sql: await loadSql(manifestNode),
      columns
    });
  }

  const collapsedNodes = [];

  for (const displayNodeId of orderedDisplayNodeIds.filter((candidateId) => graph.displayNodes.get(candidateId).kind === "collapsed")) {
    const displayNode = graph.displayNodes.get(displayNodeId);
    const boundaryTarget = displayNode.target;
    const representativeNodeId =
      boundaryTarget.defaultSelectedNodeId ||
      boundaryTarget.rootNodeIds[0] ||
      displayNode.representativeNodeIds?.[0] ||
      null;
    const representativeNode = representativeNodeId ? nodeMap[representativeNodeId] || null : null;
    const representativeYamlEntry = representativeNode ? await loadYamlEntry(representativeNode, yamlCache) : null;
    const representativeColumns = representativeNode
      ? buildColumns({
          manifestNode: representativeNode,
          yamlEntry: representativeYamlEntry,
          priorNodes: []
        })
      : [];
    const representativeCurated = representativeNode ? extractCuratedMetadata(representativeYamlEntry) : null;

    collapsedNodes.push({
      id: displayNodeId,
      name: boundaryTarget.label,
      resourceType: "dag",
      sourceStyle: false,
      layer: boundaryTarget.categoryLabel,
      depth: depthById.get(displayNodeId) || 0,
      folderLabel: boundaryTarget.folderLabel,
      description: cleanText(
        firstNonEmpty(representativeYamlEntry?.description, representativeNode?.description, boundaryTarget.subtitle)
      ),
      materialized: "dag",
      technical: {
        alias: representativeNode?.alias || boundaryTarget.key,
        schemaName: representativeNode?.schema || null,
        packageName: representativeNode?.package_name || "the_tuva_project",
        tags: representativeNode?.tags || [],
        primaryKeyColumns: representativeColumns.filter((column) => column.isPrimaryKey).map((column) => column.name)
      },
      paths: {
        sql: null,
        yaml: representativeNode ? loadYamlPath(representativeNode) : null,
        yamlEntryName: representativeNode ? loadYamlEntryName(representativeNode) : null,
        yamlCollectionKey: representativeNode ? loadYamlCollectionKey(representativeNode) : null,
        manifestNodeId: representativeNodeId,
        targetKey: boundaryTarget.key
      },
      mainDependencies: [],
      supportingDependencies: [],
      curated: {
        nodeType: representativeCurated?.nodeType || "intermediate",
        whatItRepresents: representativeCurated?.whatItRepresents || "",
        grain: representativeCurated?.grain || "",
        primaryKey: representativeCurated?.primaryKey || "",
        transformationSteps: representativeCurated?.transformationSteps || [
          `This canvas has collapsed the ${boundaryTarget.label} DAG into a single node.`,
          `Open ${boundaryTarget.label} from the selector to inspect its internal models.`
        ]
      },
      baseNodeType:
        representativeNode && representativeNodeId
          ? resolveBaseNodeType({
              manifestNode: representativeNode,
              nodeId: representativeNodeId,
              yamlEntry: representativeYamlEntry
            })
          : boundaryTarget.collapsedNodeType,
      nodeType: "intermediate",
      dagBoundary: {
        targetKey: boundaryTarget.key,
        targetLabel: boundaryTarget.label,
        categoryLabel: boundaryTarget.categoryLabel,
        memberCount: boundaryTarget.memberNodeIds.length,
        outputModels: boundaryTarget.rootNodeLabels,
        recurseWhenCollapsed: boundaryTarget.recurseWhenCollapsed,
        representativeNodeId
      },
      seedViewer: null,
      sql: "",
      columns: representativeColumns
    });
  }

  const preRoleNodes = [...modelNodes, ...seedNodes, ...collapsedNodes];
  applyContextualNodeTypes({
    nodes: preRoleNodes,
    edges: graph.edges
  });

  const nodes = preRoleNodes
    .sort((left, right) => {
      if (left.depth !== right.depth) {
        return left.depth - right.depth;
      }

      if (left.nodeType !== right.nodeType) {
        return sortNodeType(left.nodeType) - sortNodeType(right.nodeType);
      }

      return left.name.localeCompare(right.name);
    })
    .map((node, index) => ({
      ...node,
      runOrder: index + 1
    }));

  for (const node of nodes) {
    nodesById.set(node.id, node);
  }

  const edges = graph.edges
    .slice()
    .sort((left, right) => {
      const leftSourceDepth = depthById.get(left.source) || 0;
      const rightSourceDepth = depthById.get(right.source) || 0;

      if (leftSourceDepth !== rightSourceDepth) {
        return leftSourceDepth - rightSourceDepth;
      }

      if (left.target !== right.target) {
        return left.target.localeCompare(right.target);
      }

      return left.source.localeCompare(right.source);
    });

  const incomingByNodeId = new Map();

  for (const edge of edges) {
    if (!incomingByNodeId.has(edge.target)) {
      incomingByNodeId.set(edge.target, []);
    }

    incomingByNodeId.get(edge.target).push(edge.source);
  }

  for (const node of nodes) {
    const incomingIds = incomingByNodeId.get(node.id) || [];

    node.mainDependencies = incomingIds
      .map((dependencyId) => buildDependencyFromDisplayNode(nodesById.get(dependencyId)))
      .filter(Boolean);

    if (node.resourceType === "dag") {
      node.supportingDependencies = [];
      continue;
    }

    const displayNode = graph.displayNodes.get(node.id);
    const manifestNode = displayNode ? nodeMap[displayNode.actualNodeId] : null;

    node.supportingDependencies = buildSupportingDependencies({
      manifest,
      manifestNode,
      target,
      catalog,
      graph,
      visibleNodeIds: new Set(nodes.map((candidate) => candidate.id))
    });
  }

  const documentedColumns = nodes.reduce((count, node) => {
    return count + node.columns.filter((column) => column.description).length;
  }, 0);
  const hiddenSupportingDependencies = nodes.reduce((count, node) => count + node.supportingDependencies.length, 0);

  return {
    id: `${target.key}-focused-lineage`,
    generatedAt: new Date().toISOString(),
    target: {
      key: target.key,
      label: target.label,
      title: target.title,
      subtitle: target.subtitle,
      categoryKey: target.categoryKey,
      categoryLabel: target.categoryLabel,
      kind: target.kind,
      graphTitle: target.title,
      graphSubtitle: target.subtitle,
      heroNotes: [],
      defaultSelectedNodeId: target.defaultSelectedNodeId
    },
    summary: {
      hiddenSupportingDependencies,
      documentedColumns,
      generatedFrom: "manifest + YAML + SQL"
    },
    sourceArtifacts: {
      manifest: manifestPath,
      persistedPayload: getOutputPathForTarget(target.key)
    },
    nodes,
    edges
  };
}

async function buildSystemOverviewPayload({ manifest, catalog, target }) {
  const nodeMap = manifest.nodes || {};
  const yamlCache = new Map();
  const overviewTargets = catalog.targets
    .filter((candidate) => OVERVIEW_CATEGORY_ORDER[candidate.categoryKey] !== undefined)
    .slice()
    .sort((left, right) => {
      const categoryOrder = OVERVIEW_CATEGORY_ORDER[left.categoryKey] - OVERVIEW_CATEGORY_ORDER[right.categoryKey];

      if (categoryOrder !== 0) {
        return categoryOrder;
      }

      return left.label.localeCompare(right.label);
    });
  const edges = collectSystemOverviewEdges({ manifest, catalog });
  const nodes = [];
  const nodesById = new Map();

  for (const overviewTarget of overviewTargets) {
    const representativeNodeId = overviewTarget.defaultSelectedNodeId || overviewTarget.rootNodeIds[0] || null;
    const representativeNode = representativeNodeId ? nodeMap[representativeNodeId] || null : null;
    const representativeYamlEntry = representativeNode ? await loadYamlEntry(representativeNode, yamlCache) : null;
    const representativeColumns = representativeNode
      ? buildColumns({
          manifestNode: representativeNode,
          yamlEntry: representativeYamlEntry,
          priorNodes: []
        })
      : [];
    const description = cleanText(
      firstNonEmpty(representativeYamlEntry?.description, representativeNode?.description, overviewTarget.subtitle)
    );
    const node = {
      id: `dag:${overviewTarget.key}`,
      name: overviewTarget.label,
      resourceType: "dag",
      sourceStyle: false,
      layer: overviewTarget.categoryLabel,
      depth: OVERVIEW_CATEGORY_ORDER[overviewTarget.categoryKey],
      folderLabel: overviewTarget.folderLabel,
      description,
      materialized: "dag",
      technical: {
        alias: representativeNode?.alias || overviewTarget.key,
        schemaName: representativeNode?.schema || null,
        packageName: representativeNode?.package_name || "the_tuva_project",
        tags: representativeNode?.tags || [],
        primaryKeyColumns: representativeColumns.filter((column) => column.isPrimaryKey).map((column) => column.name)
      },
      paths: {
        sql: null,
        yaml: representativeNode ? loadYamlPath(representativeNode) : null,
        yamlEntryName: representativeNode ? loadYamlEntryName(representativeNode) : null,
        yamlCollectionKey: representativeNode ? loadYamlCollectionKey(representativeNode) : null,
        manifestNodeId: representativeNodeId,
        targetKey: overviewTarget.key
      },
      mainDependencies: [],
      supportingDependencies: [],
      curated: {
        nodeType: resolveOverviewNodeType(overviewTarget.categoryKey),
        whatItRepresents: "",
        grain: "",
        primaryKey: "",
        transformationSteps: [
          overviewTarget.subtitle,
          `Open ${overviewTarget.label} to inspect its internal models.`
        ].filter(Boolean)
      },
      baseNodeType: resolveOverviewNodeType(overviewTarget.categoryKey),
      nodeType: resolveOverviewNodeType(overviewTarget.categoryKey),
      dagBoundary: {
        targetKey: overviewTarget.key,
        targetLabel: overviewTarget.label,
        categoryLabel: overviewTarget.categoryLabel,
        memberCount: overviewTarget.memberNodeIds.length,
        outputModels: overviewTarget.rootNodeLabels,
        recurseWhenCollapsed: true,
        representativeNodeId
      },
      seedViewer: null,
      sql: "",
      columns: representativeColumns,
      runOrder: nodes.length + 1
    };

    nodes.push(node);
    nodesById.set(node.id, node);
  }

  const incomingByNodeId = new Map();

  for (const edge of edges) {
    if (!incomingByNodeId.has(edge.target)) {
      incomingByNodeId.set(edge.target, []);
    }

    incomingByNodeId.get(edge.target).push(edge.source);
  }

  for (const node of nodes) {
    node.mainDependencies = (incomingByNodeId.get(node.id) || [])
      .map((dependencyId) => buildDependencyFromDisplayNode(nodesById.get(dependencyId)))
      .filter(Boolean);
  }

  return {
    id: `${target.key}-overview-lineage`,
    generatedAt: new Date().toISOString(),
    target: {
      key: target.key,
      label: target.label,
      title: target.title,
      subtitle: target.subtitle,
      categoryKey: target.categoryKey,
      categoryLabel: target.categoryLabel,
      kind: target.kind,
      graphTitle: target.title,
      graphSubtitle: target.subtitle,
      heroNotes: [],
      defaultSelectedNodeId: null
    },
    summary: {
      hiddenSupportingDependencies: 0,
      documentedColumns: 0,
      generatedFrom: "manifest stage overview"
    },
    sourceArtifacts: {
      manifest: manifestPath,
      persistedPayload: getOutputPathForTarget(target.key)
    },
    nodes,
    edges
  };
}

function collectSystemOverviewEdges({ manifest, catalog }) {
  const edgeSet = new Set();
  const nodeMap = manifest.nodes || {};
  const models = Object.values(nodeMap).filter(
    (node) => node.resource_type === "model" && node.package_name === "the_tuva_project"
  );

  for (const model of models) {
    const targetBucket = resolveOverviewTargetForManifestNode(model, catalog);

    if (!targetBucket) {
      continue;
    }

    for (const dependencyId of model.depends_on?.nodes || []) {
      const dependency = nodeMap[dependencyId] || null;

      if (!dependency || dependency.resource_type !== "model") {
        continue;
      }

      const sourceBucket = resolveOverviewTargetForManifestNode(dependency, catalog);

      if (!sourceBucket || sourceBucket.key === targetBucket.key) {
        continue;
      }

      edgeSet.add(`dag:${sourceBucket.key}|||dag:${targetBucket.key}`);
    }
  }

  return Array.from(edgeSet)
    .map((value) => {
      const [source, targetId] = value.split("|||");
      return { source, target: targetId };
    })
    .sort((left, right) => {
      if (left.source !== right.source) {
        return left.source.localeCompare(right.source);
      }

      return left.target.localeCompare(right.target);
    });
}

function resolveOverviewTargetForManifestNode(manifestNode, catalog) {
  if (!manifestNode || manifestNode.resource_type !== "model" || manifestNode.package_name !== "the_tuva_project") {
    return null;
  }

  const modelPath = normalizePath(manifestNode.original_file_path || manifestNode.path || "");

  if (modelPath.startsWith("models/input_layer/")) {
    const baseName = manifestNode.name.replace(/^input_layer__/, "");
    return catalog.targetByKey.get(`input_layer__${baseName}`) || null;
  }

  if (modelPath.startsWith("models/normalization/staging/") || modelPath.startsWith("models/normalization/final/")) {
    const normalizationTargetKey = resolveClaimsNormalizationTargetKey(manifestNode.name);
    return normalizationTargetKey ? catalog.targetByKey.get(normalizationTargetKey) || null : null;
  }

  if (modelPath.startsWith("models/claims_preprocessing/service_category/")) {
    return catalog.targetByKey.get("service_categories") || null;
  }

  if (modelPath.startsWith("models/claims_preprocessing/encounters/")) {
    return catalog.targetByKey.get("encounters") || null;
  }

  if (modelPath.startsWith("models/claims_preprocessing/claims_enrollment/")) {
    return catalog.targetByKey.get("claims_enrollment") || null;
  }

  if (modelPath.startsWith("models/core/final/")) {
    return catalog.targetByKey.get(manifestNode.name.replace(/^core__/, "")) || null;
  }

  if (modelPath.startsWith("models/data_marts/")) {
    const groupName = modelPath.split("/")[2];
    return groupName ? catalog.targetByKey.get(groupName) || null : null;
  }

  return null;
}

function resolveOverviewNodeType(categoryKey) {
  if (categoryKey === "input_layer") {
    return "input";
  }

  if (categoryKey === "core" || categoryKey === "data_marts") {
    return "output";
  }

  return "intermediate";
}

export async function persistLineagePayload(payload) {
  const outputPath = getOutputPathForTarget(payload.target?.key || DEFAULT_TARGET_KEY);

  await mkdir(path.dirname(outputPath), { recursive: true });
  await writeFile(outputPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");

  return outputPath;
}

export async function readPersistedLineagePayload({ targetKey = DEFAULT_TARGET_KEY } = {}) {
  const outputPath = getOutputPathForTarget(targetKey);
  const payload = JSON.parse(await readFile(outputPath, "utf8"));

  return payload;
}

async function loadManifest() {
  return JSON.parse(await readFile(manifestPath, "utf8"));
}

function discoverTargetCatalog(manifest) {
  const models = Object.values(manifest.nodes || {}).filter(
    (node) => node.resource_type === "model" && node.package_name === "the_tuva_project"
  );
  const targets = [];
  const boundaryByNodeId = new Map();
  const inputLayerTargets = models
    .filter((node) => normalizePath(node.original_file_path).startsWith("models/input_layer/"))
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((node) => buildInputLayerTarget(node));
  const claimsNormalizationTargets = buildClaimsNormalizationTargets(models, manifest.nodes || {});

  targets.push({
    key: SYSTEM_OVERVIEW_TARGET_KEY,
    label: "Tuva Overview",
    kind: "overview",
    categoryKey: "overview",
    categoryLabel: "Overview",
    title: "Tuva Overview",
    subtitle: "See how Tuva flows from the Input Layer through claims normalization, preprocessing, core, and data marts.",
    folderLabel: "",
    recurseWhenCollapsed: false,
    collapsedNodeType: "intermediate",
    rootNodeIds: [],
    rootNodeLabels: [],
    memberNodeIds: [],
    defaultSelectedNodeId: null
  });

  for (const target of inputLayerTargets) {
    targets.push(target);
  }

  for (const target of claimsNormalizationTargets) {
    targets.push(target);
  }

  const coreTargets = models
    .filter((node) => normalizePath(node.original_file_path).startsWith("models/core/final/"))
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((node) => buildCoreTarget(node));

  for (const target of coreTargets) {
    targets.push(target);
    boundaryByNodeId.set(target.rootNodeIds[0], target);
  }

  for (const definition of fixedClaimsTargets) {
    const familyNodes = models.filter((node) => definition.matchesPath(normalizePath(node.original_file_path)));

    if (!familyNodes.length) {
      continue;
    }

    const rootNodeIds = uniqueStrings(definition.selectRootNodeIds(familyNodes));

    if (!rootNodeIds.length) {
      continue;
    }

    const target = buildFamilyTarget({
      key: definition.key,
      label: definition.label,
      kind: definition.kind,
      categoryKey: definition.categoryKey,
      categoryLabel: definition.categoryLabel,
      title: definition.title,
      subtitle: definition.subtitle,
      folderLabel: definition.folderLabel,
      recurseWhenCollapsed: definition.recurseWhenCollapsed,
      collapsedNodeType: definition.collapsedNodeType,
      rootNodeIds,
      memberNodeIds: familyNodes.map((node) => node.unique_id),
      manifestNodesById: manifest.nodes || {}
    });

    targets.push(target);

    for (const node of familyNodes) {
      boundaryByNodeId.set(node.unique_id, target);
    }
  }

  const dataMartGroups = groupDataMartNodes(models);

  for (const [groupName, groupNodes] of dataMartGroups.entries()) {
    if (groupName === "metadata") {
      continue;
    }

    const rootNodeIds = groupNodes
      .filter((node) => isPublicDataMartOutput(normalizePath(node.original_file_path), groupName))
      .map((node) => node.unique_id);

    if (!rootNodeIds.length) {
      continue;
    }

    const label = formatLabel(groupName);
    const target = buildFamilyTarget({
      key: groupName,
      label,
      kind: "data_mart",
      categoryKey: "data_marts",
      categoryLabel: "Data Marts",
      title: `${label} DAG`,
      subtitle: `Lineage for the ${label} data mart rooted at its public output models.`,
      folderLabel: `data_marts/${groupName}`,
      recurseWhenCollapsed: true,
      collapsedNodeType: "output",
      rootNodeIds,
      memberNodeIds: groupNodes.map((node) => node.unique_id),
      manifestNodesById: manifest.nodes || {}
    });

    targets.push(target);

    for (const node of groupNodes) {
      boundaryByNodeId.set(node.unique_id, target);
    }
  }

  const sortedTargets = targets.sort((left, right) => {
    const categoryOrder = sortTargetCategory(left.categoryKey) - sortTargetCategory(right.categoryKey);

    if (categoryOrder !== 0) {
      return categoryOrder;
    }

    return left.label.localeCompare(right.label);
  });

  return {
    targets: sortedTargets,
    targetByKey: new Map(sortedTargets.map((target) => [target.key, target])),
    boundaryByNodeId
  };
}

function buildCoreTarget(node) {
  const baseName = node.name.replace(/^core__/, "");
  const label = formatLabel(baseName);

  return {
    key: baseName,
    label,
    kind: "core_model",
    categoryKey: "core",
    categoryLabel: "Core",
    title: `${label} DAG`,
    subtitle: `Trace ${node.name} from its upstream sources and transformations into the final core model.`,
    folderLabel: "core/final",
    recurseWhenCollapsed: false,
    collapsedNodeType: "output",
    rootNodeIds: [node.unique_id],
    rootNodeLabels: [node.name],
    memberNodeIds: [node.unique_id],
    defaultSelectedNodeId: node.unique_id
  };
}

function buildInputLayerTarget(node) {
  const baseName = node.name.replace(/^input_layer__/, "");
  const label = formatLabel(baseName);

  return {
    key: `input_layer__${baseName}`,
    label,
    kind: "input_layer_model",
    categoryKey: "input_layer",
    categoryLabel: "Input Layer",
    title: `${label} Input Layer DAG`,
    subtitle: `Trace ${node.name} from its upstream synthetic/raw inputs into the final Input Layer model.`,
    folderLabel: "input_layer",
    recurseWhenCollapsed: false,
    collapsedNodeType: "input",
    rootNodeIds: [node.unique_id],
    rootNodeLabels: [node.name],
    memberNodeIds: [node.unique_id],
    defaultSelectedNodeId: node.unique_id
  };
}

function buildClaimsNormalizationTargets(models, manifestNodesById) {
  return [
    {
      suffix: "medical_claim",
      label: "Medical Claim"
    },
    {
      suffix: "pharmacy_claim",
      label: "Pharmacy Claim"
    },
    {
      suffix: "eligibility",
      label: "Eligibility"
    }
  ]
    .map(({ suffix, label }) => {
      const familyNodes = models.filter((node) => {
        const modelPath = normalizePath(node.original_file_path);
        return (
          (modelPath.startsWith("models/normalization/staging/") ||
            modelPath.startsWith("models/normalization/final/")) &&
          node.name.includes(suffix)
        );
      });

      if (!familyNodes.length) {
        return null;
      }

      const rootNodeIds = familyNodes
        .filter((node) => normalizePath(node.original_file_path).startsWith("models/normalization/final/"))
        .filter((node) => node.name === `normalized_input__${suffix}`)
        .map((node) => node.unique_id);

      if (!rootNodeIds.length) {
        return null;
      }

      return buildFamilyTarget({
        key: `claims_normalization__${suffix}`,
        label,
        kind: "claims_normalization",
        categoryKey: "claims_normalization",
        categoryLabel: "Claims Normalization",
        title: `${label} Claims Normalization DAG`,
        subtitle: `Normalized ${label.toLowerCase()} models that standardize Input Layer records before downstream preprocessing and core outputs.`,
        folderLabel: "normalization/final",
        recurseWhenCollapsed: true,
        collapsedNodeType: "intermediate",
        rootNodeIds,
        memberNodeIds: familyNodes.map((node) => node.unique_id),
        manifestNodesById
      });
    })
    .filter(Boolean);
}

function buildFamilyTarget({
  key,
  label,
  kind,
  categoryKey,
  categoryLabel,
  title,
  subtitle,
  folderLabel,
  recurseWhenCollapsed,
  collapsedNodeType,
  rootNodeIds,
  memberNodeIds,
  manifestNodesById
}) {
  const sortedRootNodeIds = uniqueStrings(rootNodeIds).sort((leftId, rightId) => {
    const leftName = manifestNodesById[leftId]?.name || leftId;
    const rightName = manifestNodesById[rightId]?.name || rightId;
    return leftName.localeCompare(rightName);
  });

  return {
    key,
    label,
    kind,
    categoryKey,
    categoryLabel,
    title,
    subtitle,
    folderLabel,
    recurseWhenCollapsed,
    collapsedNodeType,
    rootNodeIds: sortedRootNodeIds,
    rootNodeLabels: sortedRootNodeIds.map((nodeId) => manifestNodesById[nodeId]?.name || nodeId),
    memberNodeIds: uniqueStrings(memberNodeIds),
    defaultSelectedNodeId: sortedRootNodeIds[0] || null
  };
}

function groupDataMartNodes(models) {
  const groups = new Map();

  for (const node of models) {
    const modelPath = normalizePath(node.original_file_path);

    if (!modelPath.startsWith("models/data_marts/")) {
      continue;
    }

    const groupName = modelPath.split("/")[2];

    if (!groupName) {
      continue;
    }

    if (!groups.has(groupName)) {
      groups.set(groupName, []);
    }

    groups.get(groupName).push(node);
  }

  return groups;
}

function getTargetConfigFromCatalog(catalog, targetKey) {
  const normalizedTargetKey = targetKey === "normalization" ? "claims_normalization__medical_claim" : targetKey;
  const target = catalog.targetByKey.get(normalizedTargetKey);

  if (!target) {
    throw new Error(`Unsupported DAG target: ${targetKey}`);
  }

  return target;
}

function collectVisibleGraph({ manifest, catalog, target }) {
  const nodeMap = manifest.nodes || {};
  const displayNodes = new Map();
  const edgeSet = new Set();
  const visitedActualNodeIds = new Set();

  function addEdge(sourceId, targetId) {
    if (!sourceId || !targetId || sourceId === targetId) {
      return;
    }

    edgeSet.add(`${sourceId}|||${targetId}`);
  }

  function addDisplayNode(descriptor) {
    const existing = displayNodes.get(descriptor.id);

    if (!existing) {
      displayNodes.set(descriptor.id, descriptor);
      return descriptor;
    }

    if (descriptor.kind === "collapsed") {
      existing.representativeNodeIds = uniqueStrings([
        ...(existing.representativeNodeIds || []),
        ...(descriptor.representativeNodeIds || [])
      ]);
    }

    return existing;
  }

  function visit(nodeId) {
    const manifestNode = nodeMap[nodeId];

    if (!manifestNode) {
      return;
    }

    const descriptor = mapManifestNodeToDisplayDescriptor({
      manifestNode,
      target,
      catalog
    });

    if (!descriptor) {
      return;
    }

    addDisplayNode(descriptor);

    if (manifestNode.resource_type === "seed") {
      return;
    }

    if (shouldStopRecursingAtVisibleNode({ manifestNode, target })) {
      return;
    }

    if (visitedActualNodeIds.has(nodeId)) {
      return;
    }

    visitedActualNodeIds.add(nodeId);

    for (const dependencyId of manifestNode.depends_on?.nodes || []) {
      const dependency = nodeMap[dependencyId] || manifest.sources?.[dependencyId] || null;

      if (!dependency) {
        continue;
      }

      if (dependency.resource_type === "seed" && !isVisibleSupportingSeed(dependency, target)) {
        continue;
      }

      const dependencyDescriptor = mapManifestNodeToDisplayDescriptor({
        manifestNode: dependency,
        target,
        catalog
      });

      if (!dependencyDescriptor) {
        continue;
      }

      addDisplayNode(dependencyDescriptor);
      addEdge(dependencyDescriptor.id, descriptor.id);

      if (dependency.resource_type === "model") {
        if (dependencyDescriptor.kind === "collapsed") {
          if (dependencyDescriptor.target.recurseWhenCollapsed) {
            visit(dependencyId);
          }

          continue;
        }

        visit(dependencyId);
      }
    }
  }

  for (const rootNodeId of target.rootNodeIds) {
    visit(rootNodeId);
  }

  return {
    displayNodes,
    edges: Array.from(edgeSet).map((value) => {
      const [source, targetId] = value.split("|||");
      return { source, target: targetId };
    })
  };
}

function mapManifestNodeToDisplayDescriptor({ manifestNode, target, catalog }) {
  if (!manifestNode) {
    return null;
  }

  if (manifestNode.resource_type === "seed") {
    if (!isVisibleSupportingSeed(manifestNode, target)) {
      return null;
    }

    return {
      id: manifestNode.unique_id,
      kind: "actual",
      actualNodeId: manifestNode.unique_id
    };
  }

  if (manifestNode.resource_type !== "model") {
    return null;
  }

  const boundaryTarget = catalog.boundaryByNodeId.get(manifestNode.unique_id) || null;

  if (!boundaryTarget || boundaryTarget.key === target.key) {
    return {
      id: manifestNode.unique_id,
      kind: "actual",
      actualNodeId: manifestNode.unique_id
    };
  }

  return {
    id: `dag:${boundaryTarget.key}`,
    kind: "collapsed",
    target: boundaryTarget,
    representativeNodeIds: [manifestNode.unique_id]
  };
}

function shouldStopRecursingAtVisibleNode({ manifestNode, target }) {
  if (!manifestNode || manifestNode.resource_type !== "model" || !target) {
    return false;
  }

  if (isInputLayerModel(manifestNode) && target.categoryKey !== "input_layer") {
    return true;
  }

  const modelPath = normalizePath(manifestNode.original_file_path || manifestNode.path || "");

  if (target.categoryKey === "claims_preprocessing" && modelPath.startsWith("models/normalization/final/")) {
    return true;
  }

  return false;
}

function computeDisplayDepths(edges) {
  const parentsByTarget = new Map();
  const nodeIds = new Set();
  const memo = new Map();

  for (const edge of edges) {
    nodeIds.add(edge.source);
    nodeIds.add(edge.target);

    if (!parentsByTarget.has(edge.target)) {
      parentsByTarget.set(edge.target, []);
    }

    parentsByTarget.get(edge.target).push(edge.source);
  }

  function compute(nodeId) {
    if (memo.has(nodeId)) {
      return memo.get(nodeId);
    }

    const parents = parentsByTarget.get(nodeId) || [];

    if (!parents.length) {
      memo.set(nodeId, 0);
      return 0;
    }

    const depth = Math.max(...parents.map((parentId) => compute(parentId))) + 1;
    memo.set(nodeId, depth);
    return depth;
  }

  for (const nodeId of nodeIds) {
    compute(nodeId);
  }

  return memo;
}

function buildSupportingDependencies({ manifest, manifestNode, target, catalog, graph, visibleNodeIds }) {
  if (!manifestNode) {
    return [];
  }

  if (isInputLayerModel(manifestNode)) {
    return [];
  }

  const supportingDependencies = [];
  const seen = new Set();

  for (const dependencyId of manifestNode.depends_on?.nodes || []) {
    const dependency = manifest.nodes?.[dependencyId] || manifest.sources?.[dependencyId] || null;

    if (!dependency) {
      continue;
    }

    if (dependency.resource_type === "seed" && isVisibleSupportingSeed(dependency, target) && visibleNodeIds.has(dependencyId)) {
      continue;
    }

    if (dependency.resource_type === "model") {
      const displayDescriptor = mapManifestNodeToDisplayDescriptor({
        manifestNode: dependency,
        target,
        catalog
      });

      if (displayDescriptor?.id && visibleNodeIds.has(displayDescriptor.id) && displayDescriptor.id !== manifestNode.unique_id) {
        continue;
      }
    }

    if (!seen.has(dependencyId)) {
      supportingDependencies.push(buildDependency(manifest, dependencyId));
      seen.add(dependencyId);
    }
  }

  return supportingDependencies;
}

function buildDependencyFromDisplayNode(node) {
  if (!node) {
    return null;
  }

  return {
    id: node.id,
    name: node.name,
    resourceType: node.resourceType,
    layer: node.layer,
    folderLabel: node.folderLabel || "",
    nodeType: node.nodeType,
    path: node.paths?.sql || node.paths?.yaml || null
  };
}

function buildColumns({ manifestNode, yamlEntry, priorNodes }) {
  const yamlColumnsByName = new Map(
    (yamlEntry?.columns || []).map((column) => [column.name, normalizeYamlColumn(column)])
  );
  const manifestColumnsByName = manifestNode.columns || {};
  const orderedColumnNames = [
    ...(yamlEntry?.columns || []).map((column) => column.name),
    ...Object.keys(manifestColumnsByName).filter((columnName) => !yamlColumnsByName.has(columnName))
  ];
  const priorColumnsByName = new Map();

  for (const priorNode of priorNodes) {
    for (const column of priorNode.columns) {
      if (!priorColumnsByName.has(column.name) && hasColumnDocumentation(column)) {
        priorColumnsByName.set(column.name, {
          ...column,
          inheritedFrom: priorNode.name
        });
      }
    }
  }

  return orderedColumnNames.map((columnName) => {
    const manifestColumn = manifestColumnsByName[columnName] || {};
    const yamlColumn = yamlColumnsByName.get(columnName) || {};
    const inheritedColumn = priorColumnsByName.get(columnName) || {};
    const description = firstNonEmpty(
      cleanText(yamlColumn.description),
      cleanText(manifestColumn.description),
      cleanText(inheritedColumn.description)
    );
    const dataType = firstNonEmpty(
      yamlColumn.dataType,
      manifestColumn.data_type,
      manifestColumn.config?.meta?.data_type,
      manifestColumn.meta?.data_type,
      inheritedColumn.dataType
    );
    const terminology = firstNonEmpty(
      yamlColumn.terminology,
      manifestColumn.config?.meta?.terminology,
      manifestColumn.meta?.terminology,
      inheritedColumn.terminology
    );
    const terminologyNote = firstNonEmpty(
      yamlColumn.terminologyNote,
      manifestColumn.config?.meta?.terminology_note,
      manifestColumn.meta?.terminology_note,
      inheritedColumn.terminologyNote
    );
    const isPrimaryKey = Boolean(
      yamlColumn.isPrimaryKey ||
        manifestColumn.config?.meta?.is_primary_key ||
        manifestColumn.meta?.is_primary_key ||
        inheritedColumn.isPrimaryKey
    );
    const mappingInstructions = firstNonEmpty(
      cleanText(yamlColumn.mappingInstructions),
      cleanText(inheritedColumn.mappingInstructions)
    );
    const requiredForDataMarts = uniqueStrings([
      ...(yamlColumn.requiredForDataMarts || []),
      ...(inheritedColumn.requiredForDataMarts || [])
    ]);
    const inheritedFrom =
      !cleanText(yamlColumn.description) && !cleanText(manifestColumn.description) && inheritedColumn.inheritedFrom
        ? inheritedColumn.inheritedFrom
        : null;

    return {
      name: columnName,
      description,
      dataType,
      terminology,
      terminologyNote,
      isPrimaryKey,
      mappingInstructions,
      requiredForDataMarts,
      inheritedFrom
    };
  });
}

function normalizeYamlColumn(column) {
  return {
    name: column.name,
    description: column.description,
    mappingInstructions: column.mapping_instructions,
    requiredForDataMarts: Array.isArray(column.required_for_data_marts) ? column.required_for_data_marts : [],
    dataType: column.config?.meta?.data_type || column.meta?.data_type || null,
    terminology: column.config?.meta?.terminology || column.meta?.terminology || null,
    terminologyNote: column.config?.meta?.terminology_note || column.meta?.terminology_note || null,
    isPrimaryKey: Boolean(column.config?.meta?.is_primary_key || column.meta?.is_primary_key)
  };
}

function extractCuratedMetadata(yamlEntry) {
  const modelMeta = extractCandidateMeta(yamlEntry);

  return {
    nodeType: firstNonEmpty(
      cleanText(yamlEntry?.node_type),
      cleanText(modelMeta.node_type),
      cleanText(modelMeta.role)
    ),
    whatItRepresents: firstNonEmpty(
      cleanText(yamlEntry?.what_it_represents),
      cleanText(modelMeta.what_it_represents),
      cleanText(modelMeta.record_represents),
      cleanText(modelMeta.summary)
    ),
    grain: firstNonEmpty(
      cleanText(yamlEntry?.grain),
      cleanText(modelMeta.grain),
      cleanText(modelMeta.record_grain)
    ),
    primaryKey: yamlEntry?.primary_key || modelMeta.primary_key || "",
    transformationSteps:
      yamlEntry?.transformation_steps ||
      modelMeta.transformation_steps ||
      modelMeta.steps ||
      []
  };
}

function extractCandidateMeta(yamlEntry) {
  return {
    ...(yamlEntry?.meta || {}),
    ...(yamlEntry?.config?.meta || {}),
    ...(yamlEntry?.meta?.dag || {}),
    ...(yamlEntry?.config?.meta?.dag || {})
  };
}

function hasColumnDocumentation(column) {
  return Boolean(
    cleanText(column.description) ||
      column.dataType ||
      column.mappingInstructions ||
      column.terminology ||
      column.isPrimaryKey
  );
}

function buildDependency(manifest, dependencyId) {
  const dependency = manifest.nodes?.[dependencyId] || manifest.sources?.[dependencyId] || null;

  if (!dependency) {
    return {
      id: dependencyId,
      name: dependencyId.split(".").pop(),
      resourceType: dependencyId.split(".")[0],
      layer: "External",
      path: null
    };
  }

  return {
    id: dependencyId,
    name: dependency.name,
    resourceType: dependency.resource_type,
    layer: classifyLayer(dependency),
    folderLabel: deriveFolderLabel(dependency),
    nodeType: resolveNodeType({
      manifestNode: dependency,
      target: null,
      nodeId: dependencyId,
      yamlEntry: null,
      depth: 0
    }),
    path: dependency.path || dependency.original_file_path || null
  };
}

function classifyLayer(node) {
  if (!node) {
    return "Unknown";
  }

  const modelPath = normalizePath(node.original_file_path || node.path || "");

  if (node.package_name === "integration_tests") {
    return "Synthetic input";
  }

  if (modelPath.includes("models/input_layer/")) {
    return "Input layer";
  }

  if (modelPath.includes("models/normalization/staging/")) {
    return "Normalization staging";
  }

  if (modelPath.includes("models/normalization/final/")) {
    return "Normalization final";
  }

  if (modelPath.includes("models/core/")) {
    return "Core";
  }

  if (modelPath.includes("models/claims_preprocessing/")) {
    return "Claims preprocessing";
  }

  if (modelPath.includes("models/data_marts/")) {
    return "Data mart";
  }

  if (node.resource_type === "seed") {
    return "Seed";
  }

  return "Model";
}

function isInputLayerModel(node) {
  if (!node || node.resource_type !== "model") {
    return false;
  }

  const modelPath = normalizePath(node.original_file_path || node.path || "");
  return modelPath.startsWith("models/input_layer/");
}

function deriveFolderLabel(node) {
  if (!node) {
    return "";
  }

  const sourcePath = normalizePath(node.original_file_path || node.path || "");

  if (!sourcePath) {
    return "";
  }

  if (node.package_name === "integration_tests" && sourcePath.startsWith("models/")) {
    const relativeDirectory = path.posix.dirname(sourcePath).replace(/^models\/?/, "");
    return relativeDirectory === "." ? "" : relativeDirectory;
  }

  if (sourcePath.startsWith("models/")) {
    const relativeDirectory = path.posix.dirname(sourcePath).replace(/^models\/?/, "");
    return relativeDirectory === "." ? "" : relativeDirectory;
  }

  if (sourcePath.startsWith("seeds/")) {
    const relativeDirectory = path.posix.dirname(sourcePath).replace(/^seeds\/?/, "");
    return relativeDirectory === "." ? "seeds" : `seeds/${relativeDirectory}`;
  }

  return path.posix.dirname(sourcePath) === "." ? "" : path.posix.dirname(sourcePath);
}

function resolveBaseNodeType({ manifestNode, nodeId, yamlEntry }) {
  const manifestNodeType = firstNonEmpty(
    cleanText(manifestNode?.config?.meta?.dag?.node_type),
    cleanText(manifestNode?.meta?.dag?.node_type)
  );
  const explicitNodeType = extractCuratedMetadata(yamlEntry).nodeType;

  if (manifestNode?.resource_type === "seed") {
    return "terminology";
  }

  if (isInputLayerModel(manifestNode)) {
    return "input";
  }

  if (manifestNodeType === "input" || explicitNodeType === "input") {
    return "input";
  }

  if (manifestNode?.package_name === "integration_tests") {
    return "input";
  }

  if (manifestNodeType === "terminology" || explicitNodeType === "terminology") {
    return "terminology";
  }

  if (manifestNodeType === "output" || explicitNodeType === "output") {
    return "output";
  }

  if (manifestNodeType === "intermediate" || explicitNodeType === "intermediate") {
    return "intermediate";
  }

  if (nodeId?.startsWith("source.")) {
    return "input";
  }

  return "intermediate";
}

function resolveDisplayNodeType({ baseNodeType, target, nodeId, resourceType }) {
  if (resourceType === "seed") {
    return "terminology";
  }

  if (baseNodeType === "input") {
    return "input";
  }

  if (target?.rootNodeIds?.includes(nodeId)) {
    return "output";
  }

  return "intermediate";
}

function applyContextualNodeTypes({ nodes, edges }) {
  const nodesById = new Map(nodes.map((node) => [node.id, node]));
  const incomingByNodeId = new Map();
  const outgoingByNodeId = new Map();

  for (const node of nodes) {
    incomingByNodeId.set(node.id, 0);
    outgoingByNodeId.set(node.id, 0);
  }

  for (const edge of edges) {
    const sourceNode = nodesById.get(edge.source);
    const targetNode = nodesById.get(edge.target);

    if (!sourceNode || !targetNode) {
      continue;
    }

    if (sourceNode.resourceType === "seed" || targetNode.resourceType === "seed") {
      continue;
    }

    outgoingByNodeId.set(edge.source, (outgoingByNodeId.get(edge.source) || 0) + 1);
    incomingByNodeId.set(edge.target, (incomingByNodeId.get(edge.target) || 0) + 1);
  }

  for (const node of nodes) {
    if (node.resourceType === "seed") {
      node.nodeType = "terminology";
      continue;
    }

    const incomingCount = incomingByNodeId.get(node.id) || 0;
    const outgoingCount = outgoingByNodeId.get(node.id) || 0;

    if (outgoingCount === 0) {
      node.nodeType = "output";
      continue;
    }

    if (incomingCount === 0) {
      node.nodeType = "input";
      continue;
    }

    node.nodeType = "intermediate";
  }
}

function resolveNodeType({ manifestNode, target, nodeId, yamlEntry }) {
  return resolveDisplayNodeType({
    baseNodeType: resolveBaseNodeType({ manifestNode, nodeId, yamlEntry }),
    target,
    nodeId,
    resourceType: manifestNode?.resource_type
  });
}

function buildSeedViewer(manifestNode) {
  if (!manifestNode || manifestNode.resource_type !== "seed") {
    return null;
  }

  const normalizedPath = normalizePath(manifestNode.original_file_path || manifestNode.path || "");
  const baseDomain = "https://tuva-public-resources.s3.amazonaws.com";

  if (normalizedPath.includes("seeds/terminology/")) {
    return {
      sourceType: "seed_preview",
      family: "terminology",
      version: "latest",
      folder: "versioned_terminology",
      fileName: path.posix.basename(normalizedPath),
      downloadUrl: buildTerminologySeedDownloadUrl({ manifestNode, baseDomain })
    };
  }

  if (normalizedPath.includes("seeds/value_sets/")) {
    const fileName = buildValueSetSeedObjectFileName({ manifestNode, normalizedPath });

    return {
      sourceType: "seed_preview",
      family: "value_set",
      version: "latest",
      folder: "versioned_value_sets",
      fileName: path.posix.basename(normalizedPath),
      downloadUrl: fileName ? `${baseDomain}/versioned_value_sets/latest/${fileName}` : null
    };
  }

  return null;
}

function buildTerminologySeedDownloadUrl({ manifestNode, baseDomain }) {
  const seedName = manifestNode?.name || "";
  const datasetKey = seedName === "terminology__icd10_pcs_cms_ontology"
    ? "icd_10_pcs_cms_ontology"
    : seedName.replace(/^terminology__/, "");

  if (!datasetKey || datasetKey === "provider") {
    return null;
  }

  return `${baseDomain}/versioned_terminology/latest/${datasetKey}.csv_0_0_0.csv.gz`;
}

function buildValueSetSeedObjectFileName({ manifestNode, normalizedPath }) {
  const seedName = manifestNode?.name || path.posix.basename(normalizedPath).replace(/\.csv$/i, "");

  if (!seedName) {
    return null;
  }

  if (seedName === "encounter_group_sk" || seedName === "encounter_type_sk" || seedName === "predictor_encounter_xwalk") {
    return `${seedName}.csv.gz`;
  }

  if (seedName.startsWith("pqi__")) {
    return null;
  }

  if (seedName === "ed_classification__categories") {
    return "ed_classification_categories.csv_0_0_0.csv.gz";
  }

  if (seedName.startsWith("ed_classification__")) {
    return `${seedName.replace(/^ed_classification__/, "")}.csv_0_0_0.csv.gz`;
  }

  if (seedName.startsWith("ccsr__")) {
    return `${seedName.replace(/^ccsr__/, "")}.csv_0_0_0.csv.gz`;
  }

  if (seedName.startsWith("chronic_conditions__")) {
    return `${seedName.replace(/^chronic_conditions__/, "")}.csv_0_0_0.csv.gz`;
  }

  if (seedName === "cms_hcc__disease_hierarchy_flat") {
    return null;
  }

  if (seedName.startsWith("cms_hcc__")) {
    return `${seedName.replace(/^cms_hcc__/, "cms_hcc_")}.csv_0_0_0.csv.gz`;
  }

  if (seedName.startsWith("data_quality__")) {
    return `${seedName.replace(/^data_quality__/, "data_quality_")}.csv_0_0_0.csv.gz`;
  }

  if (seedName === "hcc_suspecting__hcc_descriptions") {
    return "hcc_suspecting_descriptions.csv_0_0_0.csv.gz";
  }

  if (seedName.startsWith("hcc_suspecting__")) {
    return `${seedName.replace(/^hcc_suspecting__/, "hcc_suspecting_")}.csv_0_0_0.csv.gz`;
  }

  if (seedName.startsWith("pharmacy__")) {
    return `${seedName.replace(/^pharmacy__/, "")}.csv_0_0_0.csv.gz`;
  }

  if (seedName === "quality_measures__value_sets") {
    return "quality_measures_value_set_codes.csv_0_0_0.csv.gz";
  }

  if (seedName.startsWith("quality_measures__")) {
    return `${seedName.replace(/^quality_measures__/, "quality_measures_")}.csv_0_0_0.csv.gz`;
  }

  if (seedName.startsWith("readmissions__")) {
    return `${seedName.replace(/^readmissions__/, "")}.csv_0_0_0.csv.gz`;
  }

  return `${seedName}.csv_0_0_0.csv.gz`;
}

function sortNodeType(nodeType) {
  const priorities = {
    input: 1,
    terminology: 2,
    intermediate: 3,
    output: 4
  };

  return priorities[nodeType] || 99;
}

async function loadYamlEntry(manifestNode, yamlCache) {
  const documentationReference = resolveDocumentationReference(manifestNode);
  const yamlPath = documentationReference?.yamlPath;

  if (!yamlPath) {
    return null;
  }

  if (!yamlCache.has(yamlPath)) {
    yamlCache.set(yamlPath, parseYaml(await readFile(yamlPath, "utf8")) || {});
  }

  const yamlDocument = yamlCache.get(yamlPath);
  const entries =
    manifestNode.resource_type === "seed"
      ? Array.isArray(yamlDocument.seeds)
        ? yamlDocument.seeds
        : []
      : Array.isArray(yamlDocument.models)
        ? yamlDocument.models
        : [];

  return entries.find((entry) => entry.name === documentationReference.entryName) || null;
}

async function loadSql(manifestNode) {
  return readFile(loadSqlPath(manifestNode), "utf8");
}

function loadYamlPath(manifestNode) {
  return resolveDocumentationReference(manifestNode)?.yamlPath || null;
}

function loadYamlEntryName(manifestNode) {
  return resolveDocumentationReference(manifestNode)?.entryName || null;
}

function loadYamlCollectionKey(manifestNode) {
  return manifestNode?.resource_type === "seed" ? "seeds" : "models";
}

function resolveDocumentationReference(manifestNode) {
  const inputLayerEntryName = resolveInputLayerEntryName(manifestNode);

  if (inputLayerEntryName) {
    const yamlPath = path.join(repoRoot, "models", "input_layer", `${inputLayerEntryName}.yml`);

    if (existsSync(yamlPath)) {
      return {
        yamlPath,
        entryName: inputLayerEntryName
      };
    }
  }

  const yamlPath = patchPathToAbsolute(manifestNode.patch_path);

  if (!yamlPath) {
    return null;
  }

  return {
    yamlPath,
    entryName: manifestNode.name
  };
}

function resolveInputLayerEntryName(manifestNode) {
  if (!manifestNode || manifestNode.resource_type !== "model") {
    return null;
  }

  if (isInputLayerModel(manifestNode)) {
    return manifestNode.name;
  }

  const tags = uniqueStrings([...(manifestNode.tags || []), ...(manifestNode.config?.tags || [])]);
  const nodeType = firstNonEmpty(
    cleanText(manifestNode.config?.meta?.dag?.node_type),
    cleanText(manifestNode.meta?.dag?.node_type)
  );

  if (manifestNode.package_name === "integration_tests" && (nodeType === "input" || tags.includes("input_layer"))) {
    return `input_layer__${manifestNode.name}`;
  }

  return null;
}

function isSourceDocumentationNode({ manifestNode, nodeId }) {
  if (!manifestNode) {
    return false;
  }

  if (nodeId?.startsWith("source.")) {
    return true;
  }

  return manifestNode.package_name === "integration_tests";
}

function loadSqlPath(manifestNode) {
  const packageRoot = packageRoots[manifestNode.package_name];

  if (!packageRoot) {
    throw new Error(`Unknown dbt package root for ${manifestNode.package_name}`);
  }

  return path.join(packageRoot, manifestNode.original_file_path);
}

function patchPathToAbsolute(patchPath) {
  if (!patchPath) {
    return null;
  }

  const [packageName, packageRelativePath] = patchPath.split("://");
  const packageRoot = packageRoots[packageName];

  if (!packageRoot) {
    return null;
  }

  return path.join(packageRoot, packageRelativePath.replace(/\?.*$/, ""));
}

function isVisibleSupportingSeed(node, target = null) {
  if (!node || node.resource_type !== "seed") {
    return false;
  }

  const seedName = node.name || "";
  const seedPath = normalizePath(node.original_file_path || node.path || "");

  return (
    seedName.startsWith("terminology__") ||
    seedName.startsWith("value_set__") ||
    seedPath.includes("/terminology/") ||
    seedPath.includes("/value_sets/")
  );
}

function isPublicDataMartOutput(modelPath, groupName) {
  if (!modelPath.startsWith(`models/data_marts/${groupName}/`)) {
    return false;
  }

  const relativeSegments = modelPath.split("/").slice(3);

  if (!relativeSegments.length || !modelPath.endsWith(".sql")) {
    return false;
  }

  return !relativeSegments.includes("staging") && !relativeSegments.includes("intermediate");
}

function normalizePath(value) {
  return String(value || "").replace(/\\/g, "/");
}

function formatLabel(value) {
  if (!value) {
    return "";
  }

  if (labelOverrides[value]) {
    return labelOverrides[value];
  }

  return value
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function sortTargetCategory(categoryKey) {
  const priorities = {
    overview: 0,
    input_layer: 1,
    claims_normalization: 2,
    claims_preprocessing: 3,
    core: 4,
    data_marts: 5
  };

  return priorities[categoryKey] || 99;
}

function resolveClaimsNormalizationTargetKey(modelName = "") {
  if (modelName.includes("medical_claim")) {
    return "claims_normalization__medical_claim";
  }

  if (modelName.includes("pharmacy_claim")) {
    return "claims_normalization__pharmacy_claim";
  }

  if (modelName.includes("eligibility")) {
    return "claims_normalization__eligibility";
  }

  return null;
}

function uniqueStrings(values) {
  return Array.from(new Set(values.filter(Boolean)));
}

function firstNonEmpty(...values) {
  for (const value of values) {
    if (Array.isArray(value) && value.length) {
      return value;
    }

    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }

    if (value && typeof value !== "string") {
      return value;
    }
  }

  return "";
}

function cleanText(value) {
  if (typeof value !== "string") {
    return "";
  }

  return value.replace(/\s+\n/g, "\n").replace(/\n{3,}/g, "\n\n").trim();
}

async function main() {
  const targetKey = parseTargetKeyFromArgv(process.argv.slice(2));
  const payload = await buildLineagePayload({ targetKey });
  const outputPath = await persistLineagePayload(payload);

  process.stdout.write(`Wrote ${outputPath}\n`);
}

function parseTargetKeyFromArgv(argv) {
  const targetIndex = argv.findIndex((argument) => argument === "--target");

  if (targetIndex >= 0 && argv[targetIndex + 1]) {
    return argv[targetIndex + 1];
  }

  if (argv[0] && !argv[0].startsWith("-")) {
    return argv[0];
  }

  return DEFAULT_TARGET_KEY;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    process.stderr.write(`${error.stack || error}\n`);
    process.exitCode = 1;
  });
}
