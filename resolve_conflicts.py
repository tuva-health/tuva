#!/usr/bin/env python3
"""
Script to resolve merge conflicts by accepting main's version for:
1. tuva_last_run column (with cast)
2. Model reference names
"""

import re
import subprocess
import sys

def resolve_conflict(content):
    """Resolve conflicts in file content"""
    # Pattern to match conflict markers
    conflict_pattern = r'<<<<<<< HEAD.*?\n(.*?)\n=======\n(.*?)\n>>>>>>> [^\n]+'
    
    def replace_conflict(match):
        head_content = match.group(1)
        main_content = match.group(2)
        
        # Check if this conflict is about tuva_last_run
        if 'tuva_last_run' in head_content or 'tuva_last_run' in main_content:
            # Prefer main's version (with cast)
            if 'cast(' in main_content and 'tuva_last_run' in main_content:
                return main_content.strip()
            elif 'cast(' in head_content and 'tuva_last_run' in head_content:
                return head_content.strip()
            # If main has tuva_last_run, prefer it
            elif 'tuva_last_run' in main_content:
                return main_content.strip()
        
        # Check if this is about ref() calls - prefer main's version
        if 'ref(' in head_content and 'ref(' in main_content:
            # Main likely has updated ref names
            return main_content.strip()
        
        # For other conflicts, prefer main's version (safer for merging)
        return main_content.strip()
    
    # Replace all conflicts
    resolved = re.sub(conflict_pattern, replace_conflict, content, flags=re.DOTALL)
    
    return resolved

def main():
    # Get list of conflicted files
    result = subprocess.run(['git', 'diff', '--name-only', '--diff-filter=U'], 
                           capture_output=True, text=True, cwd='/Users/chasejones/quality_test/tuva')
    conflicted_files = [f for f in result.stdout.strip().split('\n') if f]
    
    print(f'Resolving conflicts in {len(conflicted_files)} files...')
    
    for file_path in conflicted_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check if file has conflicts
            if '<<<<<<< HEAD' not in content:
                continue
            
            resolved_content = resolve_conflict(content)
            
            # Write resolved content
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(resolved_content)
            
            print(f'Resolved: {file_path}')
            
        except Exception as e:
            print(f'Error processing {file_path}: {e}', file=sys.stderr)
            continue

if __name__ == '__main__':
    main()

