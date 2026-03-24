import React, { useEffect, useMemo, useState } from 'react';
import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';
import { DEFAULT_BRANCH, fetchModelDefinition } from './fetchModelColumns';
import './inputLayerDictionary.css';

const ALL_TABLES_VALUE = '__all_tables__';
const ALL_DATA_MARTS_VALUE = '__all_data_marts__';
const DATA_MART_LABEL_OVERRIDES = {
  ahrq_measures: 'AHRQ Measures',
  claims_enrollment: 'Claims Enrollment',
  ccsr: 'CCSR',
  cms_chronic_conditions: 'CMS Chronic Conditions',
  cms_hccs: 'CMS HCCs',
  ed_classification: 'ED Classification',
  financial_pmpm: 'Financial PMPM',
  hcc_recapture: 'HCC Recapture',
  hcc_suspecting: 'HCC Suspecting',
  provider_attribution: 'Provider Attribution',
  quality_measures: 'Quality Measures',
  readmissions: 'Readmissions',
  semantic_layer: 'Semantic Layer',
  service_categories: 'Service Categories',
  tuva_chronic_conditions: 'Tuva Chronic Conditions',
};

const BASE_TABLE_COLUMNS = [
  { key: 'name', label: 'Column Name', className: 'input-dict-col-name' },
  { key: 'type', label: 'Data Type', className: 'input-dict-col-type' },
  { key: 'description', label: 'Description', className: 'input-dict-col-description' },
  {
    key: 'mapping_instructions',
    label: 'Mapping Instructions',
    className: 'input-dict-col-mapping',
  },
  {
    key: 'required_for_data_marts',
    label: 'Required For Data Mart',
    className: 'input-dict-col-required',
  },
];

const TABLE_COLUMN = { key: 'table', label: 'Table', className: 'input-dict-col-table' };

function truncateText(text, limit = 140) {
  if (!text) {
    return '';
  }

  if (text.length <= limit) {
    return text;
  }

  return `${text.slice(0, limit).trim()}...`;
}

function toTitleCase(value = '') {
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function formatDataMartLabel(value = '') {
  return DATA_MART_LABEL_OVERRIDES[value] || toTitleCase(value);
}

function getRequiredForDataMarts(row) {
  if (!Array.isArray(row?.required_for_data_marts)) {
    return [];
  }

  return row.required_for_data_marts;
}

function renderDataType(typeValue) {
  if (!typeValue) {
    return <span className="input-dict-type-chip input-dict-type-empty">unknown</span>;
  }

  return <span className="input-dict-type-chip">{typeValue}</span>;
}

function renderCellPreview(text, fallbackText) {
  return <span className="input-dict-cell-preview">{truncateText(text || fallbackText, 165)}</span>;
}

function renderRequiredTags(tags) {
  if (!tags.length) {
    return <span className="input-dict-empty-tag">Not tagged</span>;
  }

  return (
    <div className="input-dict-required-tags">
      {tags.map((tag) => (
        <span key={tag} className="input-dict-required-chip">
          {formatDataMartLabel(tag)}
        </span>
      ))}
    </div>
  );
}

function getVisibleRequiredTags(tags, selectedDataMart) {
  if (selectedDataMart === ALL_DATA_MARTS_VALUE) {
    return tags;
  }

  return tags.filter((tag) => tag === selectedDataMart);
}

function getRowKey(row, index) {
  if (row.__row_key) {
    return row.__row_key;
  }

  return `${row.__model_name || 'table'}::${row.name || `column-${index}`}`;
}

function normalizeTableOptions({ tableOptions, modelName, yamlPath }) {
  if (Array.isArray(tableOptions) && tableOptions.length) {
    return tableOptions;
  }

  if (modelName && yamlPath) {
    const fallbackLabel = modelName.replace(/^input_layer__/, '');
    return [
      {
        groupLabel: 'Input Layer',
        label: fallbackLabel,
        modelName,
        yamlPath,
      },
    ];
  }

  return [];
}

export default function InputLayerDictionaryTable({
  modelName,
  yamlPath,
  tableOptions,
  defaultModelName = 'input_layer__eligibility',
  branch = DEFAULT_BRANCH,
  showMappingInstructions = true,
  showRequiredForDataMart = true,
}) {
  const normalizedOptions = useMemo(
    () => normalizeTableOptions({ tableOptions, modelName, yamlPath }),
    [tableOptions, modelName, yamlPath]
  );

  const defaultSelection = useMemo(() => {
    if (!normalizedOptions.length) {
      return '';
    }

    const explicitDefault = normalizedOptions.find((option) => option.modelName === defaultModelName);
    if (explicitDefault) {
      return explicitDefault.modelName;
    }

    return normalizedOptions[0].modelName;
  }, [normalizedOptions, defaultModelName]);

  const [selectedModelName, setSelectedModelName] = useState(defaultSelection || ALL_TABLES_VALUE);
  const [selectedDataMart, setSelectedDataMart] = useState(ALL_DATA_MARTS_VALUE);
  const [tableDataByModel, setTableDataByModel] = useState({});
  const [searchInput, setSearchInput] = useState('');
  const [expandedRow, setExpandedRow] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    setSelectedModelName(defaultSelection || ALL_TABLES_VALUE);
  }, [defaultSelection]);

  useEffect(() => {
    if (!ExecutionEnvironment.canUseDOM) {
      return undefined;
    }

    if (!normalizedOptions.length) {
      setLoading(false);
      setError('No table has been configured.');
      return undefined;
    }

    let isMounted = true;
    setLoading(true);
    setError(null);

    Promise.all(
      normalizedOptions.map((option) =>
        fetchModelDefinition({
          modelName: option.modelName,
          yamlPath: option.yamlPath,
          branch,
        }).then(({ modelDescription, columns }) => {
          const rows = (columns || []).map((row, index) => ({
            ...row,
            __group_label: option.groupLabel,
            __table_label: option.label,
            __model_name: option.modelName,
            __row_key: `${option.modelName}::${row.name || `column-${index}`}::${index}`,
          }));

          const primaryKeyColumns = rows
            .filter((column) => column.is_primary_key === 'Yes')
            .map((column) => column.name)
            .filter(Boolean);

          return {
            modelName: option.modelName,
            value: {
              ...option,
              modelDescription: modelDescription || '',
              rows,
              primaryKeyColumns,
            },
          };
        })
      )
    )
      .then((entries) => {
        if (!isMounted) {
          return;
        }

        const dataMap = entries.reduce((accumulator, entry) => {
          accumulator[entry.modelName] = entry.value;
          return accumulator;
        }, {});

        setTableDataByModel(dataMap);
        setLoading(false);
      })
      .catch((err) => {
        if (isMounted) {
          setTableDataByModel({});
          setError(err.message);
          setLoading(false);
        }
      });

    return () => {
      isMounted = false;
    };
  }, [normalizedOptions, branch]);

  const selectedTable = useMemo(() => {
    return tableDataByModel[selectedModelName] || null;
  }, [tableDataByModel, selectedModelName]);

  const allRows = useMemo(() => {
    return normalizedOptions.flatMap((option) => tableDataByModel[option.modelName]?.rows || []);
  }, [normalizedOptions, tableDataByModel]);

  const availableDataMarts = useMemo(() => {
    if (!showRequiredForDataMart) {
      return [];
    }

    const marts = new Set();

    allRows.forEach((row) => {
      getRequiredForDataMarts(row).forEach((tag) => marts.add(tag));
    });

    return Array.from(marts).sort();
  }, [allRows, showRequiredForDataMart]);

  const scopedRows = useMemo(() => {
    if (selectedModelName === ALL_TABLES_VALUE) {
      return allRows;
    }

    return selectedTable?.rows || [];
  }, [selectedModelName, allRows, selectedTable]);

  const filteredRows = useMemo(() => {
    const rowsAfterMartFilter =
      !showRequiredForDataMart || selectedDataMart === ALL_DATA_MARTS_VALUE
        ? scopedRows
        : scopedRows.filter((row) => getRequiredForDataMarts(row).includes(selectedDataMart));

    if (!searchInput.trim()) {
      return rowsAfterMartFilter;
    }

    const filterValue = searchInput.toLowerCase();

    return rowsAfterMartFilter.filter((row) => {
      const valuesToSearch = [
        row.__table_label,
        row.__group_label,
        row.name,
        row.type,
        row.description,
        row.full_description,
      ];

      if (showMappingInstructions) {
        valuesToSearch.push(row.mapping_instructions);
      }

      if (showRequiredForDataMart) {
        valuesToSearch.push(getRequiredForDataMarts(row).join(' '));
      }

      return valuesToSearch.some((value) => value && String(value).toLowerCase().includes(filterValue));
    });
  }, [scopedRows, selectedDataMart, searchInput, showMappingInstructions, showRequiredForDataMart]);

  const groupedRows = useMemo(() => {
    if (selectedModelName !== ALL_TABLES_VALUE) {
      return [];
    }

    return normalizedOptions
      .map((option) => ({
        table: tableDataByModel[option.modelName],
        rows: filteredRows.filter((row) => row.__model_name === option.modelName),
      }))
      .filter((group) => group.rows.length > 0);
  }, [selectedModelName, normalizedOptions, tableDataByModel, filteredRows]);

  useEffect(() => {
    if (!expandedRow) {
      return;
    }

    const rowIsVisible = filteredRows.some((row, index) => getRowKey(row, index) === expandedRow);
    if (!rowIsVisible) {
      setExpandedRow(null);
    }
  }, [filteredRows, expandedRow]);

  if (error) {
    return <p role="alert">Unable to load dictionary columns: {error}</p>;
  }

  const showAllTables = selectedModelName === ALL_TABLES_VALUE;
  const configuredColumns = BASE_TABLE_COLUMNS.filter((column) => {
    if (column.key === 'mapping_instructions') {
      return showMappingInstructions;
    }

    if (column.key === 'required_for_data_marts') {
      return showRequiredForDataMart;
    }

    return true;
  });
  const activeColumns = showAllTables ? [TABLE_COLUMN, ...configuredColumns] : configuredColumns;

  const gridTemplateColumns = (() => {
    const columns = [];

    if (showAllTables) {
      columns.push('minmax(110px, 0.75fr)');
    }

    columns.push('minmax(180px, 1.2fr)');
    columns.push('minmax(100px, 0.75fr)');
    columns.push('minmax(260px, 1.9fr)');

    if (showMappingInstructions) {
      columns.push('minmax(230px, 1.85fr)');
    }

    if (showRequiredForDataMart) {
      columns.push('minmax(220px, 1.6fr)');
    }

    return columns.join(' ');
  })();

  const renderTableRows = (rows) =>
    rows.map((row, rowIndex) => {
      const rowKey = getRowKey(row, rowIndex);
      const isExpanded = expandedRow === rowKey;
      const isPrimaryKey = row.is_primary_key === 'Yes';
      const requiredDataMarts = getRequiredForDataMarts(row);
      const visibleRequiredDataMarts = showRequiredForDataMart
        ? getVisibleRequiredTags(requiredDataMarts, selectedDataMart)
        : [];

      return (
        <div key={rowKey} className={`input-dict-record ${isExpanded ? 'is-expanded' : ''}`}>
          <button
            type="button"
            className={`input-dict-row ${showAllTables ? 'has-table' : ''} ${
              isExpanded ? 'is-expanded' : ''
            }`}
            onClick={() => setExpandedRow(isExpanded ? null : rowKey)}
            style={{ gridTemplateColumns }}
          >
            {showAllTables ? (
              <div className="input-dict-cell input-dict-col-table">
                <span className="input-dict-table-pill">{row.__table_label}</span>
              </div>
            ) : null}

            <div className="input-dict-cell input-dict-col-name">
              <div className="input-dict-column-name">
                <code>{row.name}</code>
                {isPrimaryKey ? (
                  <span className="input-dict-pk-chip" aria-label="Primary key column">
                    PK
                  </span>
                ) : null}
              </div>
            </div>

            <div className="input-dict-cell input-dict-col-type">{renderDataType(row.type)}</div>

            <div className="input-dict-cell input-dict-col-description">
              {renderCellPreview(row.description || row.full_description, 'No description provided.')}
            </div>

            {showMappingInstructions ? (
              <div className="input-dict-cell input-dict-col-mapping">
                {renderCellPreview(
                  row.mapping_instructions,
                  'No explicit mapping instructions provided.'
                )}
              </div>
            ) : null}

            {showRequiredForDataMart ? (
              <div className="input-dict-cell input-dict-col-required">
                {renderRequiredTags(visibleRequiredDataMarts)}
              </div>
            ) : null}
          </button>

          {isExpanded ? (
            <div className="input-dict-inline-expanded">
              <div className="input-dict-expanded-meta">
                <div className="input-dict-expanded-meta-item">
                  <div className="input-dict-expanded-label">Column Name</div>
                  <div className="input-dict-column-name">
                    <code>{row.name}</code>
                    {row.is_primary_key === 'Yes' ? (
                      <span className="input-dict-pk-chip" aria-label="Primary key column">
                        PK
                      </span>
                    ) : null}
                  </div>
                </div>
                <div className="input-dict-expanded-meta-item">
                  <div className="input-dict-expanded-label">Data Type</div>
                  {renderDataType(row.type)}
                </div>
              </div>

              <div className="input-dict-expanded-stack">
                <section className="input-dict-expanded-section">
                  <h5>Description</h5>
                  <p>{row.full_description || 'No description provided.'}</p>
                </section>
                {showMappingInstructions ? (
                  <section className="input-dict-expanded-section">
                    <h5>Mapping Instructions</h5>
                    <p>{row.mapping_instructions || 'No explicit mapping instructions provided.'}</p>
                  </section>
                ) : null}
                {showRequiredForDataMart ? (
                  <section className="input-dict-expanded-section">
                    <h5>Required For Data Mart</h5>
                    <div>{renderRequiredTags(visibleRequiredDataMarts)}</div>
                  </section>
                ) : null}
              </div>
            </div>
          ) : null}
        </div>
      );
    });

  return (
    <div className="input-dict-wrapper">
      <div
        className={`input-dict-toolbar input-dict-toolbar-main ${
          showRequiredForDataMart ? '' : 'input-dict-toolbar-no-mart'
        }`}
      >
        <div className="input-dict-control input-dict-control-table">
          <label htmlFor="input-dict-table-selector">Select Table</label>
          <select
            id="input-dict-table-selector"
            value={selectedModelName}
            onChange={(event) => {
              setSelectedModelName(event.target.value);
              setExpandedRow(null);
            }}
            className="input-dict-table-selector"
          >
            <option value={ALL_TABLES_VALUE}>All Tables</option>
            {Array.from(new Set(normalizedOptions.map((option) => option.groupLabel || 'Input Layer'))).map(
              (groupLabel) => (
                <optgroup key={groupLabel} label={groupLabel}>
                  {normalizedOptions
                    .filter((option) => (option.groupLabel || 'Input Layer') === groupLabel)
                    .map((option) => (
                      <option key={option.modelName} value={option.modelName}>
                        {option.label}
                      </option>
                    ))}
                </optgroup>
              )
            )}
          </select>
        </div>

        {showRequiredForDataMart ? (
          <div className="input-dict-control input-dict-control-mart">
            <label htmlFor="input-dict-data-mart-selector">Required For Data Mart</label>
            <select
              id="input-dict-data-mart-selector"
              value={selectedDataMart}
              onChange={(event) => {
                setSelectedDataMart(event.target.value);
                setExpandedRow(null);
              }}
              className="input-dict-table-selector"
            >
              <option value={ALL_DATA_MARTS_VALUE}>All Data Marts</option>
              {availableDataMarts.map((mart) => (
                <option key={mart} value={mart}>
                  {formatDataMartLabel(mart)}
                </option>
              ))}
            </select>
          </div>
        ) : null}

        <label className="input-dict-search-wrapper" htmlFor="input-dict-dictionary-search">
          <svg className="input-dict-search-icon" viewBox="0 0 20 20" aria-hidden="true" focusable="false">
            <path
              d="M13.9 12.9l3.5 3.5a.75.75 0 0 1-1.06 1.06l-3.5-3.5a6 6 0 1 1 1.06-1.06zM8.5 13a4.5 4.5 0 1 0 0-9 4.5 4.5 0 0 0 0 9z"
              fill="currentColor"
            />
          </svg>
          <input
            id="input-dict-dictionary-search"
            type="text"
            value={searchInput}
            onChange={(event) => setSearchInput(event.target.value)}
            placeholder={
              showMappingInstructions
                ? 'Search column, type, description, mapping'
                : 'Search column, type, description'
            }
            className="input-dict-search"
          />
        </label>

        <span className="input-dict-count">{filteredRows.length} columns</span>
      </div>

      {!showAllTables && selectedTable ? (
        <section className="input-dict-table-context">
          <p className="input-dict-section-label">{selectedTable.groupLabel || 'Input Layer'}</p>
          <h3 className="input-dict-table-name">{selectedTable.label || 'Table'}</h3>
          {selectedTable.modelDescription ? (
            <p className="input-dict-model-description">{selectedTable.modelDescription}</p>
          ) : null}
          <div className="input-dict-pk-row">
            <span className="input-dict-pk-label">Primary Key Columns</span>
            {selectedTable.primaryKeyColumns.length ? (
              <div className="input-dict-pk-list">
                {selectedTable.primaryKeyColumns.map((columnName) => (
                  <code key={columnName}>{columnName}</code>
                ))}
              </div>
            ) : (
              <span className="input-dict-pk-none">None defined.</span>
            )}
          </div>
        </section>
      ) : null}

      <div className="input-dict-grid">
        <div
          className={`input-dict-grid-header ${showAllTables ? 'has-table' : ''}`}
          role="row"
          style={{ gridTemplateColumns }}
        >
          {activeColumns.map((column) => (
            <div key={column.key} className={`input-dict-cell ${column.className}`}>
              {column.label}
            </div>
          ))}
        </div>

        {loading ? <div className="input-dict-empty">Loading columns...</div> : null}

        {!loading && filteredRows.length === 0 ? (
          <div className="input-dict-empty">No columns match your filters.</div>
        ) : null}

        {!loading && showAllTables
          ? groupedRows.map((group) => (
              <div key={group.table.modelName} className="input-dict-group-wrap">
                <div className="input-dict-group-header">
                  <div className="input-dict-group-title-wrap">
                    <span className="input-dict-group-label">{group.table.groupLabel}</span>
                    <span className="input-dict-group-title">{group.table.label}</span>
                    {group.table.primaryKeyColumns.length ? (
                      <div className="input-dict-group-pk-list">
                        <span className="input-dict-group-pk-label">PK</span>
                        {group.table.primaryKeyColumns.map((columnName) => (
                          <code key={`${group.table.modelName}-${columnName}`}>{columnName}</code>
                        ))}
                      </div>
                    ) : null}
                  </div>
                  <span className="input-dict-group-count">{group.rows.length} columns</span>
                </div>
                {renderTableRows(group.rows)}
              </div>
            ))
          : null}

        {!loading && !showAllTables ? renderTableRows(filteredRows) : null}
      </div>
    </div>
  );
}
