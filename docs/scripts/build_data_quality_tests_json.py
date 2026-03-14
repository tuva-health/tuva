#!/usr/bin/env python3
import json
import yaml
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MODELS_ROOT = REPO_ROOT / 'tuva/models'
OUTPUT_PATH = REPO_ROOT / 'docs/src/data/dataQualityTests.json'


def find_test_lines(file_path):
    lines = file_path.read_text().splitlines()
    column_lines = {}
    current_column = None
    collecting = False
    tests_indent = None

    for idx, line in enumerate(lines, start=1):
        stripped = line.lstrip()
        indent = len(line) - len(stripped)

        if stripped.startswith('- name:'):
            current_column = stripped.split(':', 1)[1].strip()
            collecting = False
            continue

        if current_column and stripped.startswith('tests:'):
            collecting = True
            tests_indent = indent
            column_lines.setdefault(current_column, [])
            continue

        if collecting:
            if stripped.startswith('- ') and indent == (tests_indent or 0) + 2:
                column_lines.setdefault(current_column, []).append(idx)
                continue
            if stripped == '' or stripped.startswith('#'):
                continue
            if indent <= tests_indent:
                collecting = False

    return column_lines


def main():
    rows = []
    for path in MODELS_ROOT.rglob('*.yml'):
        rel_path = path.relative_to(REPO_ROOT)
        try:
            data = yaml.safe_load(path.read_text())
        except Exception:
            continue
        if not isinstance(data, dict) or 'models' not in data:
            continue
        line_lookup = find_test_lines(path)
        for model in data.get('models', []):
            model_name = model.get('name')
            if not model_name:
                continue
            columns = model.get('columns') or []
            for column in columns:
                column_name = column.get('name')
                if not column_name:
                    continue
                tests = column.get('tests') or []
                test_lines = line_lookup.get(column_name, [])
                for idx, test in enumerate(tests):
                    if isinstance(test, str):
                        test_name = test
                        description = ''
                        severity = ''
                    elif isinstance(test, dict):
                        test_name, details = next(iter(test.items()))
                        description = ''
                        severity = ''
                        if isinstance(details, dict):
                            description = details.get('description') or details.get('config', {}).get('description', '')
                            severity = details.get('config', {}).get('severity', '')
                    else:
                        continue
                    rows.append({
                        'schemaName': model_name.split('__')[0] if model_name and '__' in model_name else 'Unknown',
                        'modelName': model_name,
                        'columnName': column_name,
                        'testName': test_name,
                        'description': description,
                        'severity': severity,
                        'yamlPath': str(rel_path),
                        'lineNumber': test_lines[idx] if idx < len(test_lines) else None,
                    })
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(rows, f, indent=2)
    print(f'Wrote {len(rows)} test rows to {OUTPUT_PATH}')


if __name__ == '__main__':
    main()
