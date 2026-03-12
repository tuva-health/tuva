import React, { useEffect, useState } from 'react';
import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';
import { DEFAULT_BRANCH, fetchModelDefinition } from './fetchModelColumns';

const DEFAULT_YAML_PATH = 'models/core/core_models.yml';

async function loadModelDetails(modelName, yamlPath, branch) {
  const { modelDescription, columns } = await fetchModelDefinition({
    modelName,
    yamlPath,
    branch: branch || DEFAULT_BRANCH,
  });

  const primaryKeyColumns = (columns || [])
    .filter((column) => column.is_primary_key === 'Yes')
    .map((column) => column.name)
    .filter(Boolean);

  return {
    modelDescription,
    primaryKeyColumns,
  };
}

export function TableDescription({
  modelName,
  yamlPath = DEFAULT_YAML_PATH,
  branch = DEFAULT_BRANCH,
  showPrimaryKeyColumns = false,
}) {
  const [description, setDescription] = useState('');
  const [primaryKeyColumns, setPrimaryKeyColumns] = useState([]);
  const [detailsLoaded, setDetailsLoaded] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!ExecutionEnvironment.canUseDOM) {
      return undefined;
    }

    let isMounted = true;
    setError(null);
    setDescription('');
    setPrimaryKeyColumns([]);
    setDetailsLoaded(false);

    loadModelDetails(modelName, yamlPath, branch)
      .then(({ modelDescription, primaryKeyColumns: pkColumns }) => {
        if (isMounted) {
          setDescription(modelDescription);
          setPrimaryKeyColumns(pkColumns);
          setDetailsLoaded(true);
        }
      })
      .catch((err) => {
        if (isMounted) {
          setError(err.message);
          setDetailsLoaded(true);
        }
      });

    return () => {
      isMounted = false;
    };
  }, [modelName, yamlPath, branch]);

  if (error) {
    return <p role="alert">Unable to load description: {error}</p>;
  }

  if (!description && !showPrimaryKeyColumns) {
    return null;
  }

  const shouldRenderPrimaryKeys = showPrimaryKeyColumns && detailsLoaded;

  return (
    <>
      {description ? <p>{description}</p> : null}
      {shouldRenderPrimaryKeys ? (
        <div>
          <p>
            <strong>Primary Key Columns</strong>
          </p>
          {primaryKeyColumns.length ? (
            <ul>
              {primaryKeyColumns.map((columnName) => (
                <li key={columnName}>
                  <code>{columnName}</code>
                </li>
              ))}
            </ul>
          ) : (
            <p>None defined.</p>
          )}
        </div>
      ) : null}
    </>
  );
}

export default TableDescription;
