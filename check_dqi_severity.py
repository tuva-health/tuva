#!/usr/bin/env python3
"""
DQI Severity Level Checker

Scans YAML files for dbt tests with tuva_dqi_sev_1 tags and ensures 
they have severity: error instead of severity: warn.

Usage:
    python scripts/check_dqi_severity.py
    
Exit codes:
    0: No violations found
    1: Violations found
    2: Script error
"""

import os
import sys
import yaml
import argparse
from pathlib import Path
from typing import List, Dict, Any, Tuple

def find_yaml_files(root_dir: str) -> List[Path]:
    """Find all YAML files in the models directory."""
    models_dir = Path(root_dir) / "models"
    if not models_dir.exists():
        print(f"Models directory not found: {models_dir}")
        sys.exit(2)
    
    yaml_files = []
    # recursively add each yml file
    for yaml_file in models_dir.rglob("*.yml"):
        yaml_files.append(yaml_file)
    
    print(f"Found {len(yaml_files)} YAML files to check")
    return yaml_files

def parse_yaml_safely(file_path: Path) -> Dict[Any, Any]:
    """Parse YAML file safely, return empty dict on error."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"Could not parse {file_path}: {e}")
        return {}

def extract_tests_from_yaml(yaml_content: Dict[Any, Any], file_path: Path) -> List[Dict]:
    """Extract test configurations from YAML content."""
    tests = []
    
    if not isinstance(yaml_content, dict):
        return tests
    
    # Check models section
    models = yaml_content.get('models', [])
    if not isinstance(models, list):
        return tests
    
    for model in models:
        if not isinstance(model, dict):
            continue
            
        # Check columns
        columns = model.get('columns', [])
        if isinstance(columns, list):
            for column in columns:
                if isinstance(column, dict):
                    column_tests = column.get('tests', [])
                    if isinstance(column_tests, list):
                        for test in column_tests:
                            if isinstance(test, dict):
                                tests.append({
                                    'test': test,
                                    'file_path': file_path,
                                    'context': f"model: {model.get('name', 'unknown')}, column: {column.get('name', 'unknown')}"
                                })
        
        # Check model-level tests
        model_tests = model.get('tests', [])
        if isinstance(model_tests, list):
            for test in model_tests:
                if isinstance(test, dict):
                    tests.append({
                        'test': test,
                        'file_path': file_path,
                        'context': f"model: {model.get('name', 'unknown')}"
                    })
    
    return tests

def check_test_severity(test_config: Dict) -> Tuple[bool, Dict]:
    """
    Check if a test has tuva_dqi_sev_1 tag and incorrect severity.
    
    Returns:
        (is_violation, violation_info)
    """
    # Extract test name and configuration
    test_name = None
    config = {}
    tags = []
    
    # Handle different test formats
    if isinstance(test_config, dict):
        if len(test_config) == 1:
            # Format: {test_name: {config}}
            test_name = list(test_config.keys())[0]
            test_details = test_config[test_name]
            if isinstance(test_details, dict):
                config = test_details.get('config', {})
                tags = test_details.get('tags', [])
        else:
            # Direct config format
            config = test_config.get('config', {})
            tags = test_config.get('tags', [])
            test_name = test_config.get('test', 'unknown')
    
    # Check if this test has tuva_dqi_sev_1 tag
    has_sev_1_tag = False
    if isinstance(tags, list):
        has_sev_1_tag = any('tuva_dqi_sev_1' in str(tag) for tag in tags)
    
    if not has_sev_1_tag:
        return False, {}
    
    # Check severity configuration
    severity = config.get('severity', 'default')
    
    if severity == 'warn':
        return True, {
            'test_name': test_name,
            'current_severity': severity,
            'expected_severity': 'error',
            'tags': tags,
            'config': config
        }
    
    return False, {}

def find_violations(yaml_files: List[Path]) -> List[Dict]:
    """Find all DQI severity violations across YAML files."""
    violations = []
    
    for yaml_file in yaml_files:
        
        yaml_content = parse_yaml_safely(yaml_file)
        tests = extract_tests_from_yaml(yaml_content, yaml_file)
        
        for test_info in tests:
            is_violation, violation_info = check_test_severity(test_info['test'])
            if is_violation:
                violations.append({
                    'file_path': test_info['file_path'],
                    'context': test_info['context'],
                    **violation_info
                })
    
    return violations

def print_violations(violations: List[Dict]) -> None:
    """Print violations in a user-friendly format."""
    if not violations:
        print("No DQI severity level violations found!")
        return
    
    print(f"Found {len(violations)} DQI Severity Level Violation(s)\n")
    print("The following tests have severity level 1 but are configured with 'warn' instead of 'error':\n")
    
    for i, violation in enumerate(violations, 1):
        print(f"{i}. {violation['file_path']}")
        print(f"   Context: {violation['context']}")
        print(f"   Test: {violation['test_name']}")
        print(f"   Current: severity: {violation['current_severity']}")
        print(f"   Expected: severity: {violation['expected_severity']}")
        print()
    
    print("Severity Level 1 tests should fail builds (error), not just warn.")
    print("This ensures critical data quality issues stop the pipeline.")

def main():
    parser = argparse.ArgumentParser(description='Check DQI severity levels in YAML files')
    parser.add_argument('--root-dir', default='.', help='Root directory to search (default: current directory)')
    
    args = parser.parse_args()
    
    print("DQI Severity Level Checker")
    print("=" * 40)
    
    # Find YAML files
    yaml_files = find_yaml_files(args.root_dir)
    
    if not yaml_files:
        print("No YAML files found to check")
        sys.exit(2)
    
    # Find violations
    violations = find_violations(yaml_files)
    
    # Print results
    print_violations(violations)

    # Print summary count at the end
    print("=" * 40)
    if violations:
        print(f"SUMMARY: {len(violations)} tests with incorrect severity found")
        print("   These tests are tagged 'tuva_dqi_sev_1' but have 'severity: warn' instead of 'severity: error'")
    else:
        print("SUMMARY: All severity level 1 tests are correctly configured")
    
    # Exit with appropriate code
    if violations:
        sys.exit(1)  # Violations found
    else:
        sys.exit(0)  # No violations

if __name__ == "__main__":
    main()
