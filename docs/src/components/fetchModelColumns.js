import yaml from 'js-yaml';

const RAW_BASE_URLS = [
  'https://raw.githubusercontent.com/tuva-health/tuva',
  'https://raw.githubusercontent.com/tuva-health/the_tuva_project',
];
export const DEFAULT_BRANCH = 'main';
const YAML_LOAD_OPTIONS = { json: true };

function normalizeYamlPath(relativePath) {
  if (!relativePath) {
    throw new Error('yamlPath must be provided');
  }

  const cleanedPath = relativePath.replace(/^\/+/, '');

  if (!cleanedPath.startsWith('models/')) {
    throw new Error('yamlPath must reside within the "models" directory');
  }

  return cleanedPath;
}

function buildSourceUrls(cleanedPath, branch = DEFAULT_BRANCH) {
  const urls = [];

  // Use local repo files first so docs changes are testable without pushing.
  if (cleanedPath.startsWith('models/')) {
    urls.push(`/${cleanedPath.replace(/^models\//, '')}`);
    urls.push(`/${cleanedPath.split('/').pop()}`);
  }

  RAW_BASE_URLS.forEach((baseUrl) => {
    urls.push(`${baseUrl}/${branch}/${cleanedPath}`);
  });

  return urls;
}

export async function fetchYamlFile(yamlPath, branch = DEFAULT_BRANCH) {
  const cleanedPath = normalizeYamlPath(yamlPath);
  let lastError;
  const sourceUrls = buildSourceUrls(cleanedPath, branch);

  for (const url of sourceUrls) {
    
    try {
      const response = await fetch(url, { cache: 'no-cache' });
      if (!response.ok) {
        lastError = new Error(`Failed to fetch ${url}: ${response.status}`);
        continue;
      }

      return {
        text: await response.text(),
        url,
      };
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error(`Failed to fetch ${cleanedPath} from all configured sources`);
}

function normalizeDescription(text) {
  if (!text) {
    return '';
  }

  return text
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .join(' ');
}

function normalizeRequiredForDataMarts(rawValue) {
  if (!rawValue) {
    return [];
  }

  if (Array.isArray(rawValue)) {
    return rawValue
      .map((value) => String(value || '').trim())
      .filter(Boolean)
      .map((value) => value.toLowerCase())
      .filter((value, index, array) => array.indexOf(value) === index)
      .sort();
  }

  if (typeof rawValue === 'string') {
    return rawValue
      .split(',')
      .map((value) => value.trim())
      .filter(Boolean)
      .map((value) => value.toLowerCase())
      .filter((value, index, array) => array.indexOf(value) === index)
      .sort();
  }

  return [];
}

function mapColumns(columns = []) {
  return columns.map((column) => {
    const normalizedDescription = normalizeDescription(column.description);
    const requiredForDataMarts = normalizeRequiredForDataMarts(
      column.config?.meta?.required_for_data_marts ||
        column.meta?.required_for_data_marts ||
        column.required_for_data_marts
    );

    return {
      name: column.name,
      type:
        column.config?.meta?.data_type ||
        column.meta?.data_type ||
        column.config?.data_type ||
        column.data_type ||
        '',
      description: normalizedDescription,
      full_description: normalizedDescription,
      required_for_data_marts: requiredForDataMarts,
      is_primary_key:
        column.config?.meta?.is_primary_key === true || column.meta?.is_primary_key === true
          ? 'Yes'
          : 'No',
    };
  });
}

function hasUsableColumnMetadata(columns = []) {
  return columns.some((column) => column.type || column.description);
}

export async function fetchModelDefinition({ modelName, yamlPath, branch = DEFAULT_BRANCH }) {
  if (!modelName) {
    throw new Error('modelName must be provided');
  }

  const cleanedPath = normalizeYamlPath(yamlPath);
  let fallbackMatch = null;
  let lastError = null;
  const sourceUrls = buildSourceUrls(cleanedPath, branch);

  for (const url of sourceUrls) {

    try {
      const response = await fetch(url, { cache: 'no-cache' });
      if (!response.ok) {
        lastError = new Error(`Failed to fetch ${url}: ${response.status}`);
        continue;
      }

      const parsed = yaml.load(await response.text(), YAML_LOAD_OPTIONS);
      const model = parsed?.models?.find((entry) => entry.name === modelName);
      if (!model || !Array.isArray(model.columns)) {
        continue;
      }

      const columns = mapColumns(model.columns);
      const modelDescription = normalizeDescription(model.description);
      const usableMetadata = hasUsableColumnMetadata(columns) || Boolean(modelDescription);

      const currentMatch = { model, columns, modelDescription, url };
      if (usableMetadata) {
        return currentMatch;
      }

      // Keep a model match even if metadata is sparse, in case no better source exists.
      fallbackMatch = currentMatch;
    } catch (error) {
      lastError = error;
    }
  }

  if (fallbackMatch) {
    return fallbackMatch;
  }

  throw lastError || new Error(`Model "${modelName}" not found in ${yamlPath}`);
}

export async function fetchModelColumns({ modelName, yamlPath, branch = DEFAULT_BRANCH }) {
  const { columns } = await fetchModelDefinition({ modelName, yamlPath, branch });
  return columns;
}

export default fetchModelColumns;
