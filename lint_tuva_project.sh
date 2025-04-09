#!/usr/bin/env bash
# ==============================================================================
# Script: lint_tuva_project.sh
# Description: Runs dbt, compiles the project, and then uses SQLFluff to lint
#              and optionally fix SQL files within the dbt project.
#              Includes specific logic for handling integration test files and
#              temporarily modifying dbt_project.yml for compilation.
#              Designed for both local development and CI environments.
#
# Usage:
#   Local (Default - runs fix): ./lint_tuva_project.sh
#   Local (Lint only, no fix): ./lint_tuva_project.sh --no-fix
#   CI Mode (Lint only, fails on issues): ./lint_tuva_project.sh --ci
#
# Prerequisites:
#   - Bash shell (standard on Linux/macOS, available via Git Bash or WSL on Windows)
#   - dbt-core installed and configured for the project
#   - sqlfluff and sqlfluff-templater-dbt installed
#   - Must be run from the root of the dbt project directory.
#
# Outputs:
#   - Logs filtered linting/fixing output to SQLFLUFF_LINTER_OUTPUT.TXT
#   - Prints progress and filtered results to the terminal.
#   - Exits with 0 on success, 1 on failure (lint errors or script errors).
#
# Notes:
#   - Temporarily modifies dbt_project.yml (backs up as dbt_project.yml.bak).
#   - Copies/removes files in models/integration_tests and seeds/integration_tests.
#   - Use the --ci flag for automated checks where fixing is not desired.
# ==============================================================================

# Exit on error, treat unset variables as error, exit on pipeline failure
set -euo pipefail

# --- Configuration ---
SQLFLUFF_OUTPUT_FILE="SQLFLUFF_LINTER_OUTPUT.TXT"

# --- Script Flags (Defaults for Local Use) ---
MODE="local" # 'local' or 'ci'
RUN_FIX=true # Whether to run 'sqlfluff fix'

# --- Argument Parsing ---
# Simple loop for basic flags. Use getopt for more complex needs.
while [[ $# -gt 0 ]]; do
  case $1 in
    --ci)
      MODE="ci"
      RUN_FIX=false # In CI, we don't want 'fix' to be the goal, just the check
      echo "Running in CI mode."
      shift # past argument
      ;;
    --no-fix)
      RUN_FIX=false
      echo "SQLFluff fix command will be skipped."
      shift # past argument
      ;;
    *) # unknown option
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

# --- Functions ---

# Helper to log output to file and terminal (filtered)
_log_sqlfluff_output() {
    local phase="$1"
    local output="$2"
    local filter="$3"
    # Append header to file
    echo -e "\n===== SQLFLUFF $phase OUTPUT ($(date +%Y-%m-%d_%H:%M:%S)) (Filtered) =====\n" >> "$SQLFLUFF_OUTPUT_FILE"
    # Append filtered output to file
    echo "$output" | grep -E -v "$filter" >> "$SQLFLUFF_OUTPUT_FILE"
    echo -e "\n=========================================================================\n" >> "$SQLFLUFF_OUTPUT_FILE"
    # Display filtered output to terminal as well
    echo "--- SQLFluff $phase Output (Filtered) ---"
    echo "$output" | grep -E -v "$filter"
    echo "-----------------------------------------"
}

# Function to check if running in a git repository
check_git_directory() {
  if ! git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
    echo "Warning: Script does not appear to be running in a git repository." >&2
    exit 1
  fi
}

# Function to run dbt debug
run_dbt_debug() {
  local dbt_output
  # Capture stderr to stdout for unified output capture
  if ! dbt_output=$(dbt debug 2>&1); then
    echo "Error: dbt debug failed. A valid dbt connection is required." >&2
    echo "$dbt_output" >&2 # Show debug failure output
    exit 1
  fi
  # Return the output for the confirmation prompt
  echo "$dbt_output"
}

# Function to run dbt clean
run_dbt_clean() {
  echo "Running dbt clean..."
  if ! dbt clean; then
    echo "Error: dbt clean failed." >&2
    exit 1
  fi
  echo "dbt clean completed successfully."
}

# Function to run dbt deps
run_dbt_deps() {
  echo "Running dbt deps..."
  if ! dbt deps; then
    echo "Error: dbt deps failed. Unable to install dependencies." >&2
    exit 1
  fi
  echo "dbt deps completed successfully."
}

# Function to prompt user confirmation (Only runs in local mode)
prompt_user_confirmation() {
  local debug_output="$1"

  # Only prompt if MODE is 'local'
  if [[ "$MODE" == "local" ]]; then
    echo -e "\n==== DBT Debug Output ===="
    echo "$debug_output" # Show relevant part of dbt debug
    echo -e "==========================\n"

    # Use default=N and handle case-insensitivity
    read -r -p "Are you sure you're NOT running in a production environment and want to proceed? [y/N]: " response
    response=$(echo "$response" | tr '[:upper:]' '[:lower:]')

    if [[ "$response" != "y" ]]; then
      echo "Operation cancelled by user."
      exit 0
    fi
    echo "Proceeding..."
  else
    echo "Skipping user confirmation prompt in CI mode."
  fi
}

# Function to ensure directory exists
ensure_directory_exists() {
   local directory="$1"
   if [ ! -d "$directory" ]; then
     mkdir -p "$directory"
     echo "Created directory: $directory"
   fi
}

# Function to copy files recursively
copy_files_recursively() {
  local source_dir="$1"
  local target_dir="$2"

  if [ ! -d "$source_dir" ]; then
    echo "Source directory does not exist: $source_dir"
    return 1 # Indicate failure
  fi

  ensure_directory_exists "$target_dir"

  # Check if target has contents before removing
  if [ -n "$(ls -A "$target_dir" 2>/dev/null)" ]; then
      echo "Removing existing contents from $target_dir"
      # Using :? to ensure this never expands to /* if var is unset (though set -u helps)
      rm -rf "${target_dir:?}"/*
  else
      echo "Target directory $target_dir is empty, no removal needed."
  fi

  echo "Copying files from $source_dir to $target_dir..."
  # Use -v for verbose output if desired, or handle errors more explicitly
  if cp -r "$source_dir"/* "$target_dir"/; then
      echo "Successfully copied files from $source_dir to $target_dir"
      return 0 # Indicate success
  else
      echo "Error copying files from $source_dir to $target_dir" >&2
      return 1 # Indicate failure
  fi
}

# Function to copy integration files (Use modified copy_files_recursively)
copy_integration_files() {
  echo -e "\nCopying integration test files..."
  local success=true
  local models_source="integration_tests/models"
  local models_target="models/integration_tests"
  local seeds_source="integration_tests/seeds"
  local seeds_target="seeds/integration_tests"

  copy_files_recursively "$models_source" "$models_target" || success=false
  copy_files_recursively "$seeds_source" "$seeds_target" || success=false

  if [[ "$success" != true ]]; then
      echo "Error occurred during integration file copying." >&2
      exit 1
  fi
}

# Function to add variables to dbt_project.yml
add_variables_to_dbt_project() {
  echo -e "\nAdding variables to dbt_project.yml..."
  # Create a backup of the original dbt_project.yml
  if [ ! -f dbt_project.yml ]; then
      echo "Error: dbt_project.yml not found in current directory." >&2
      exit 1
  fi
  cp dbt_project.yml dbt_project.yml.bak

  # Check if vars section already exists
  if grep -q -E "^\s*vars:" dbt_project.yml; then
    # Add our variables under the existing vars section (handles indentation better)
    # Using awk for more robust YAML modification (basic example)
    awk '
    BEGIN { printing=1; added=0 }
    /^\s*vars:/ { print; print "    clinical_enabled: true"; print "    claims_enabled: true"; print "    provider_attribution_enabled: true"; print "    use_synthetic_data: false"; print "    input_database: dev_ci_testing"; print "    input_schema: input_layer"; added=1; next }
    # If another top-level key is found after vars potentially, stop adding
    /^[a-zA-Z0-9_]+:/ && added { printing=0 }
    printing { print }
    ' dbt_project.yml > dbt_project.yml.temp && mv dbt_project.yml.temp dbt_project.yml
  else
    # Add a new vars section at the end
    cat >> dbt_project.yml << EOF

vars:
  clinical_enabled: true
  claims_enabled: true
  provider_attribution_enabled: true
  use_synthetic_data: false
  input_database: dev_ci_testing
  input_schema: input_layer
EOF
  fi
  echo "Variables potentially added/updated in dbt_project.yml."
}

# Function to restore original dbt_project.yml
restore_dbt_project() {
  echo -e "\nRestoring original dbt_project.yml..."
  if [ -f dbt_project.yml.bak ]; then
    mv dbt_project.yml.bak dbt_project.yml
    echo "Original dbt_project.yml restored."
  else
    echo "Warning: Could not find backup of dbt_project.yml (dbt_project.yml.bak)." >&2
  fi
}

# Function to run dbt compile
run_dbt_compile() {
  if ! dbt compile; then
    echo "Error: dbt compile failed." >&2
    # Cleanup is handled by trap
    exit 1 # Exit immediately as compile failure is critical
  fi
  echo "dbt compile completed successfully."
}

# Function to run sqlfluff based on mode and flags
run_sqlfluff() {
  local final_exit_code=0
  local fix_status=0
  local lint_status=0
  local fix_output=""
  local lint_output=""

  # Clear previous output file if it exists
  # Append mode/time info to the top for context
  echo "SQLFluff Run Log - Mode: $MODE, Fix: $RUN_FIX, Time: $(date)" > "$SQLFLUFF_OUTPUT_FILE"

  local filter_pattern='^WARNING[[:space:]]+(File .* was not found in dbt project|Skipped file .* because it is disabled)[[:space:]]*$'

  echo "--- Running SQLFluff ---"
  echo "Mode: $MODE"
  echo "Attempt Fix: $RUN_FIX"
  echo "------------------------"

  if [[ "$MODE" == "ci" ]]; then
      # --- CI Mode Logic ---
      # Fail if initial lint finds issues. 'fix' is only run to log potential changes/errors.
      echo -e "\n[CI Mode] Running sqlfluff lint (initial check)..."
      lint_output=$(sqlfluff lint . 2>&1) || lint_status=$?
      _log_sqlfluff_output "LINT (Initial Check)" "$lint_output" "$filter_pattern"

      if [ $lint_status -ne 0 ]; then
          echo "[CI Mode] Initial lint found issues (exit code $lint_status). Failing CI step." >&2
          final_exit_code=1

          # Optionally run fix just to see/log its output if lint failed
          echo -e "\n[CI Mode] Running sqlfluff fix (for logging purposes, outcome already determined)..."
          fix_output=$(sqlfluff fix . 2>&1) || fix_status=$?
          _log_sqlfluff_output "FIX (Check, Informational)" "$fix_output" "$filter_pattern"
          # We don't change final_exit_code based on fix_status in CI mode here
      else
          echo "[CI Mode] Initial lint check passed."
          final_exit_code=0
      fi

  else
      # --- Local Mode Logic ---
      if [[ "$RUN_FIX" == true ]]; then
          # Run fix, then lint
          echo -e "\n[Local Mode] Running sqlfluff fix..."
          # Add --force to potentially fix more, be cautious
          fix_output=$(sqlfluff fix . 2>&1) || fix_status=$?
          _log_sqlfluff_output "FIX" "$fix_output" "$filter_pattern"

          if [ $fix_status -eq 1 ]; then
              echo "[Local Mode] sqlfluff fix modified files. Please review and commit."
              # Continue to lint check
          elif [ $fix_status -gt 1 ]; then
              echo "[Local Mode] Error during sqlfluff fix (exit code $fix_status). Linting may be unreliable." >&2
              final_exit_code=1 # Mark as failed due to fix error
          fi

          # Run lint after fix (unless fix had a critical error)
          if [ $final_exit_code -eq 0 ]; then
               echo -e "\n[Local Mode] Running sqlfluff lint (after fix)..."
               lint_output=$(sqlfluff lint . 2>&1) || lint_status=$?
               _log_sqlfluff_output "LINT (After Fix)" "$lint_output" "$filter_pattern"

               if [ $lint_status -ne 0 ]; then
                   echo "[Local Mode] Lint issues remain after fix (exit code $lint_status)." >&2
                   final_exit_code=1 # Mark as failed
               else
                   if [ $fix_status -eq 0 ]; then
                       echo "[Local Mode] Lint check passed (no fixes needed or applied)."
                   else # fix_status was 1
                       echo "[Local Mode] Lint check passed (after fixes were applied)."
                   fi
                   # final_exit_code remains 0
               fi
          fi
      else
          # Local mode, but --no-fix was specified
          echo -e "\n[Local Mode] Running sqlfluff lint only (--no-fix specified)..."
          lint_output=$(sqlfluff lint . 2>&1) || lint_status=$?
          _log_sqlfluff_output "LINT (Only)" "$lint_output" "$filter_pattern"

          if [ $lint_status -ne 0 ]; then
              echo "[Local Mode] Lint check found issues (exit code $lint_status)." >&2
              final_exit_code=1 # Mark as failed
          else
              echo "[Local Mode] Lint check passed."
              final_exit_code=0
          fi
      fi
  fi

  echo -e "\nSQLFluff execution finished. Filtered output logged to $SQLFLUFF_OUTPUT_FILE"
  return $final_exit_code
}


# Function to remove directory if it exists
remove_directory_if_exists() {
  local directory="$1"
  if [ -d "$directory" ]; then
    echo "Removing directory: $directory"
    # Using :? to ensure this never expands to / if var is unset
    if rm -rf "${directory:?}"; then
        echo "Successfully removed $directory"
        return 0
    else
        echo "Error removing directory: $directory" >&2
        return 1
    fi
  else
    # It's okay if it doesn't exist during cleanup
    # echo "Directory does not exist, no need to remove: $directory"
    return 0 # Return success if already gone
  fi
}

# Function to clean up integration files (Use modified remove_directory_if_exists)
cleanup_integration_files() {
  echo -e "\nCleaning up integration test files..."
  local models_target="models/integration_tests"
  local seeds_target="seeds/integration_tests"
  remove_directory_if_exists "$models_target"
  remove_directory_if_exists "$seeds_target"
}


# --- Cleanup Function ---
cleanup() {
  local exit_status=${1:-$?} # Use provided status or last command's status
  echo -e "\n--- Running Cleanup (Script exit status: $exit_status) ---"
  restore_dbt_project
  cleanup_integration_files
  echo "--- Cleanup Finished ---"
  # Exit with the original script exit status
  exit "$exit_status"
}

# --- Main Execution ---
main() {
  # Set up trap to call cleanup function on EXIT, INT, TERM
  # Pass the exit status to the cleanup function
  trap 'cleanup $?' EXIT INT TERM

  # Check git directory (optional, provides warning)
  check_git_directory

  # Run dbt debug and capture output for potential confirmation
  # Suppress debug output showing in main log, only show if error or needed for prompt
  debug_output=$(run_dbt_debug)

  # Run dbt clean to remove the target directory
  run_dbt_clean

  # Run dbt deps to install dependencies
  run_dbt_deps

  # Prompt user ONLY if in local mode
  prompt_user_confirmation "$debug_output"

  # --- Core Logic ---
  copy_integration_files
  add_variables_to_dbt_project # Creates .bak file
  run_dbt_compile

  # Run sqlfluff based on mode/flags
  if ! run_sqlfluff; then
    # run_sqlfluff returns non-zero
    echo -e "\nLinting process completed with issues detected. Check logs above and $SQLFLUFF_OUTPUT_FILE." >&2
    exit 1
  else
    # run_sqlfluff returns 0
    echo -e "\nLinting process completed successfully!"
  fi

}

# --- Run Main ---
# Clear the log file at the start of a new run
> "$SQLFLUFF_OUTPUT_FILE"
echo "Starting Lint Script - Mode: $MODE, Fix: $RUN_FIX, Time: $(date)" > "$SQLFLUFF_OUTPUT_FILE"
echo "Log file: $SQLFLUFF_OUTPUT_FILE"

main
