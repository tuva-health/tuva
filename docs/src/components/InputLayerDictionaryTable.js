import React, { useEffect, useMemo, useRef, useState } from 'react';
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
    key: 'required_for_data_marts',
    label: 'Impacts Data Marts',
    className: 'input-dict-col-required',
  },
];

const TABLE_COLUMN = { key: 'table', label: 'Table', className: 'input-dict-col-table' };

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
    return <span className="input-dict-type-text input-dict-type-empty">unknown</span>;
  }

  return <span className="input-dict-type-text">{typeValue}</span>;
}

function renderCellPreview(text, fallbackText) {
  return <span className="input-dict-cell-preview">{text || fallbackText}</span>;
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

function flattenOptionGroups(optionGroups = []) {
  return optionGroups.flatMap((group) => group.options || []);
}

function DictionarySelect({ id, label, value, optionGroups, onChange, className = '' }) {
  const [isOpen, setIsOpen] = useState(false);
  const wrapperRef = useRef(null);
  const options = useMemo(() => flattenOptionGroups(optionGroups), [optionGroups]);
  const selectedOption = options.find((option) => option.value === value) || options[0];

  useEffect(() => {
    if (!isOpen) {
      return undefined;
    }

    const handleClickAway = (event) => {
      if (!wrapperRef.current?.contains(event.target)) {
        setIsOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickAway);

    return () => {
      document.removeEventListener('mousedown', handleClickAway);
    };
  }, [isOpen]);

  return (
    <div className={`input-dict-control input-dict-custom-select ${className}`} ref={wrapperRef}>
      <label id={`${id}-label`} htmlFor={id}>
        {label}
      </label>
      <button
        aria-expanded={isOpen}
        aria-haspopup="listbox"
        aria-labelledby={`${id}-label ${id}`}
        className="input-dict-select-trigger"
        id={id}
        onClick={() => setIsOpen((current) => !current)}
        onKeyDown={(event) => {
          if (event.key === 'Escape') {
            setIsOpen(false);
          }
        }}
        type="button"
      >
        <span className="input-dict-select-value">{selectedOption?.label || 'Select'}</span>
        <span className={`input-dict-select-caret ${isOpen ? 'is-open' : ''}`} aria-hidden="true" />
      </button>

      {isOpen ? (
        <div
          aria-labelledby={`${id}-label`}
          className="input-dict-select-menu"
          role="listbox"
          tabIndex={-1}
        >
          {optionGroups.map((group, groupIndex) => (
            <div className="input-dict-select-group" key={group.label || `group-${groupIndex}`}>
              {group.label ? <div className="input-dict-select-group-label">{group.label}</div> : null}
              {(group.options || []).map((option) => {
                const isSelected = option.value === value;
                return (
                  <button
                    aria-selected={isSelected}
                    className={`input-dict-select-option ${isSelected ? 'is-selected' : ''}`}
                    key={option.value}
                    onClick={() => {
                      onChange(option.value);
                      setIsOpen(false);
                    }}
                    role="option"
                    type="button"
                  >
                    <span className="input-dict-select-option-label">{option.label}</span>
                  </button>
                );
              })}
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
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

  const tableSelectGroups = useMemo(() => {
    const groupLabels = Array.from(
      new Set(normalizedOptions.map((option) => option.groupLabel || 'Input Layer'))
    );

    return [
      {
        options: [{ label: 'All Tables', value: ALL_TABLES_VALUE }],
      },
      ...groupLabels.map((groupLabel) => ({
        label: groupLabel,
        options: normalizedOptions
          .filter((option) => (option.groupLabel || 'Input Layer') === groupLabel)
          .map((option) => ({
            label: option.label,
            value: option.modelName,
          })),
      })),
    ];
  }, [normalizedOptions]);

  const dataMartSelectGroups = useMemo(
    () => [
      {
        options: [
          { label: 'All Data Marts', value: ALL_DATA_MARTS_VALUE },
          ...availableDataMarts.map((mart) => ({
            label: formatDataMartLabel(mart),
            value: mart,
          })),
        ],
      },
    ],
    [availableDataMarts]
  );

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

      if (showRequiredForDataMart) {
        valuesToSearch.push(getRequiredForDataMarts(row).join(' '));
      }

      return valuesToSearch.some((value) => value && String(value).toLowerCase().includes(filterValue));
    });
  }, [scopedRows, selectedDataMart, searchInput, showRequiredForDataMart]);

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
    if (column.key === 'required_for_data_marts') {
      return showRequiredForDataMart;
    }

    return true;
  });
  const activeColumns = showAllTables ? [TABLE_COLUMN, ...configuredColumns] : configuredColumns;

  const gridTemplateColumns = (() => {
    const columns = [];

    if (showAllTables) {
      columns.push('minmax(150px, 0.8fr)');
    }

    columns.push('minmax(220px, 1fr)');
    columns.push('minmax(130px, 0.55fr)');
    columns.push('minmax(420px, 2fr)');

    if (showRequiredForDataMart) {
      columns.push('minmax(300px, 1.35fr)');
    }

    return columns.join(' ');
  })();

  const gridMinWidth = (() => {
    let minWidth = showAllTables ? 150 : 0;
    minWidth += 220 + 130 + 420;
    if (showRequiredForDataMart) {
      minWidth += 300;
    }
    return `${minWidth}px`;
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
            style={{ gridTemplateColumns, minWidth: gridMinWidth }}
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
                {showRequiredForDataMart ? (
                  <section className="input-dict-expanded-section">
                    <h5>Impacts Data Marts</h5>
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
        <DictionarySelect
          className="input-dict-control-table"
          id="input-dict-table-selector"
          label="Select Table"
          onChange={(nextValue) => {
            setSelectedModelName(nextValue);
            setExpandedRow(null);
          }}
          optionGroups={tableSelectGroups}
          value={selectedModelName}
        />

        {showRequiredForDataMart ? (
          <DictionarySelect
            className="input-dict-control-mart"
            id="input-dict-data-mart-selector"
            label="Data Mart Impact"
            onChange={(nextValue) => {
              setSelectedDataMart(nextValue);
              setExpandedRow(null);
            }}
            optionGroups={dataMartSelectGroups}
            value={selectedDataMart}
          />
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
            placeholder="Search column, type, description"
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
          style={{ gridTemplateColumns, minWidth: gridMinWidth }}
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
