import React, { useState, useEffect, useMemo } from 'react';
import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';
import './tableStyles.css'; // Import your existing CSS file
import { DEFAULT_BRANCH, fetchModelColumns } from './fetchModelColumns';

// Change this value to point every ExpandableTable at a different branch by default.
const EXPANDABLE_TABLE_BRANCH = DEFAULT_BRANCH;

// Generate a short preview for the collapsed row
const getPreview = (content) => {
    if (!content) {
        return '';
    }
    const normalized = content.replace(/\s+/g, ' ').trim();
    if (normalized.length <= 160) {
        return normalized;
    }
    return `${normalized.slice(0, 157).trim()}...`;
};

// Convert nested table data into the format expected by the table
const extractColumnList = (tableData) => {
    if (!tableData) {
        return [];
    }

    const normalizeEntry = (entry) => {
        if (!entry) {
            return null;
        }
        if (typeof entry === 'string') {
            return null;
        }
        const name = entry.name || entry.column || '';
        const guidance = entry.guidance || entry.description || entry.content || '';
        if (!name) {
            return null;
        }
        return { name, guidance };
    };

    if (Array.isArray(tableData)) {
        return tableData
            .map((entry) => normalizeEntry(entry) || {
                name: entry?.name,
                guidance: typeof entry === 'string' ? entry : entry?.guidance,
            })
            .filter((entry) => entry && entry.name);
    }

    if (typeof tableData === 'object') {
        if (Array.isArray(tableData.columns)) {
            return tableData.columns
                .map((entry) => normalizeEntry(entry) || {
                    name: entry?.name,
                    guidance: typeof entry === 'string' ? entry : entry?.guidance,
                })
                .filter((entry) => entry && entry.name);
        }

        if (tableData.columns && typeof tableData.columns === 'object') {
            return Object.entries(tableData.columns)
                .filter(([columnName]) => columnName.toLowerCase() !== 'table overview')
                .map(([columnName, content]) => ({
                    name: columnName,
                    guidance: content,
                }));
        }

        return Object.entries(tableData)
            .filter(([columnName]) => columnName.toLowerCase() !== 'table overview')
            .map(([columnName, content]) => ({
                name: columnName,
                guidance: content,
            }));
    }

    return [];
};

const formatTableData = (tableData) => {
    const columns = extractColumnList(tableData);
    return columns.map(({ name, guidance }) => ({
        concept_name: name,
        concept_type: getPreview(guidance),
        concept_scope: guidance,
    }));
};

const parseJsonData = (jsonDataMan, jsonDataCat, jsonPath) => {
    if (!jsonDataMan || !jsonDataCat || !jsonPath) {
        return [];
    }
    const pathSegments = jsonPath.split(/(?<!\\)\./);
    const dataMan = pathSegments.reduce((acc, path) => {
        const unescapedPath = path.replace(/\\\./g, '.');
        return acc[unescapedPath];
    }, jsonDataMan);
    const dataCat = pathSegments.reduce((acc, path) => {
        const unescapedPath = path.replace(/\\\./g, '.');
        return acc[unescapedPath];
    }, jsonDataCat);

    const parsedDataCat = Object.entries(dataCat).map(([key, value]) => ({
        name: key,
        type: value.type,
        index: value.index,
    }));

    const parsedDataMan = Object.entries(dataMan).map(([key, value]) => ({
        name: key,
        description: value.description,
        data_type: value.data_type,
    }));

    return parsedDataCat.map((catEntry) => {
        const matched = parsedDataMan.find(
            (obj) => obj.name && obj.name.toLowerCase() === catEntry.name.toLowerCase()
        );
        return {
            name: matched ? matched.name : catEntry.name,
            type: matched && matched.data_type
                ? matched.data_type
                : catEntry.type === 'TEXT'
                    ? 'varchar'
                    : catEntry.type.toLowerCase(),
            description: matched ? matched.description : undefined,
        };
    });
};

const truncateText = (text, limit = 100) => {
    if (!text) {
        return '';
    }
    if (text.length <= limit) {
        return text;
    }
    return `${text.slice(0, limit).trim()}…`;
};

const metadataColumns = [
    { key: 'name', label: 'Column Name', className: 'wide-column' },
    { key: 'type', label: 'Data Type', className: 'narrow-column' },
    { key: 'description', label: 'Description', className: 'description-column' },
];

const defaultColumns = [
    { key: 'concept_name', label: 'Concept Name', className: 'fixed-column' },
    { key: 'concept_type', label: 'Concept Type', className: 'expandable-column' },
];

function ExpandableTable({
    dataSourceUrl,
    tableData,
    jsonPath,
    modelName,
    yamlPath,
    branch = EXPANDABLE_TABLE_BRANCH,
}) {
    const [data, setData] = useState([]);
    const [metadata, setMetadata] = useState([]);
    const [expandedRow, setExpandedRow] = useState(null);  // State to track expanded row
    const [searchInput, setSearchInput] = useState('');  // State for the search input

    useEffect(() => {
        if (!dataSourceUrl) {
            return;
        }
        const fetchData = async () => {
            try {
                const response = await fetch(dataSourceUrl);
                const jsonData = await response.json();
                setData(jsonData);
            } catch (error) {
                console.error('Failed to fetch data:', error);
            }
        };

        fetchData();
    }, [dataSourceUrl]);

    useEffect(() => {
        if (!ExecutionEnvironment.canUseDOM) {
            return;
        }

        if (!jsonPath && !useYamlMetadata) {
            setMetadata([]);
            return;
        }

        let isMounted = true;

        const fetchMetadata = async () => {
            try {
                if (useYamlMetadata) {
                    const columns = await fetchModelColumns({ modelName, yamlPath, branch });
                    if (isMounted) {
                        setMetadata(columns);
                    }
                    return;
                }

                const [responseMan, responseCat] = await Promise.all([
                    fetch('https://tuva-health.github.io/tuva/manifest.json'),
                    fetch('https://tuva-health.github.io/tuva/catalog.json'),
                ]);
                const jsonDataMan = await responseMan.json();
                const jsonDataCat = await responseCat.json();
                const parsed = parseJsonData(jsonDataMan, jsonDataCat, jsonPath);
                if (isMounted) {
                    setMetadata(parsed);
                }
            } catch (error) {
                if (isMounted) {
                    setMetadata([]);
                }
                console.error('Failed to fetch metadata:', error);
            }
        };

        fetchMetadata();

        return () => {
            isMounted = false;
        };
    }, [jsonPath, modelName, yamlPath, branch]);

    const tableDataMap = useMemo(() => {
        if (!tableData) {
            return null;
        }
        const columns = extractColumnList(tableData);
        if (!columns.length) {
            return null;
        }
        return columns.reduce((acc, column) => {
            if (column.name && column.guidance) {
                acc[column.name] = column.guidance;
                acc[column.name.toLowerCase()] = column.guidance;
            }
            return acc;
        }, {});
    }, [tableData]);

    const useYamlMetadata = Boolean(modelName && yamlPath);
    const shouldUseMetadata = metadata.length > 0;

    const tableRows = useMemo(() => {
        if (shouldUseMetadata) {
            return metadata.map((row) => {
                const fullDescription = row.description || '';
                return {
                    ...row,
                    description: truncateText(fullDescription),
                    concept_scope: fullDescription,
                };
            });
        }
        if (useYamlMetadata) {
            return [];
        }
        if (tableData) {
            return formatTableData(tableData);
        }
        return data;
    }, [data, tableData, metadata, shouldUseMetadata, useYamlMetadata]);

    const filteredData = useMemo(() => {
        const lowercasedFilter = searchInput.toLowerCase();
        return tableRows.filter((row) =>
            Object.values(row).some((value) =>
                value && String(value).toLowerCase().includes(lowercasedFilter)
            )
        );
    }, [tableRows, searchInput]);

    const columns = useYamlMetadata ? metadataColumns : defaultColumns;
    const renderExpandedContent = (content) =>
        content ? content : 'No additional context available for this column.';

    return (
        <>
            <input
                type="text"
                value={searchInput}
                onChange={e => setSearchInput(e.target.value)}
                placeholder="Search..."
                style={{ marginBottom: '10px', width: '100%', padding: '8px' }}
            />
            <table className="custom-expandable-table">
                <thead>
                    <tr>
                        {columns.map((column) => (
                            <th key={column.key} className={column.className}>
                                {column.label}
                            </th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {filteredData.map((row, index) => {
                        const baseKey = row.name || row.concept_name || `row-${index}`;
                        const rowKey = `${baseKey}-${index}`;
                        return (
                            <React.Fragment key={rowKey}>
                                <tr onClick={() => setExpandedRow(expandedRow === rowKey ? null : rowKey)}>
                                    {columns.map((column) => (
                                        <td key={column.key} className={column.className}>
                                            {row[column.key]}
                                        </td>
                                    ))}
                                </tr>
                                {expandedRow === rowKey && (
                                    <tr>
                                        <td colSpan={columns.length}>
                                            <pre style={{ whiteSpace: 'pre-wrap' }}>
                                                {renderExpandedContent(row.concept_scope)}
                                            </pre>
                                        </td>
                                    </tr>
                                )}
                            </React.Fragment>
                        );
                    })}
                </tbody>
            </table>
        </>
    );
}

export default ExpandableTable;
