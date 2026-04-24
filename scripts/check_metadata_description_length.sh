#!/usr/bin/env bash
set -euo pipefail

LIMIT=1024
REPO_ROOT="."
ROOTS=()
YQ_BIN="${YQ_BIN:-yq}"
JQ_BIN="${JQ_BIN:-jq}"

usage() {
  cat <<'EOF'
Usage: scripts/check_metadata_description_length.sh [--limit N] [--repo-root PATH] [--root PATH...]

Scans dbt schema YAML metadata under models/, seeds/, and snapshots/ and fails if any
column description exceeds the configured limit.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --limit)
      LIMIT="$2"
      shift 2
      ;;
    --repo-root)
      REPO_ROOT="$2"
      shift 2
      ;;
    --root)
      ROOTS+=("$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! command -v "$YQ_BIN" >/dev/null 2>&1; then
  echo "Missing required dependency: $YQ_BIN" >&2
  exit 2
fi

if ! command -v "$JQ_BIN" >/dev/null 2>&1; then
  echo "Missing required dependency: $JQ_BIN" >&2
  exit 2
fi

if [[ ${#ROOTS[@]} -eq 0 ]]; then
  ROOTS=(models seeds snapshots)
fi

cd "$REPO_ROOT"

FILE_LIST=()
while IFS= read -r -d '' file_path; do
  FILE_LIST+=("$file_path")
done < <(
  for root in "${ROOTS[@]}"; do
    if [[ -d "$root" ]]; then
      find "$root" -type f \( -name '*.yml' -o -name '*.yaml' \) -print0
    fi
  done | sort -z
)

if [[ ${#FILE_LIST[@]} -eq 0 ]]; then
  echo "No schema YAML files found under: ${ROOTS[*]}"
  exit 0
fi

readonly YQ_EXPRESSION='
  (
    .models[]? as $resource
    | $resource.columns[]?
    | select(has("description"))
    | {
        "file": filename,
        "resource_type": "model",
        "resource_name": $resource.name,
        "column_name": .name,
        "description": .description
      }
  ),
  (
    .seeds[]? as $resource
    | $resource.columns[]?
    | select(has("description"))
    | {
        "file": filename,
        "resource_type": "seed",
        "resource_name": $resource.name,
        "column_name": .name,
        "description": .description
      }
  ),
  (
    .snapshots[]? as $resource
    | $resource.columns[]?
    | select(has("description"))
    | {
        "file": filename,
        "resource_type": "snapshot",
        "resource_name": $resource.name,
        "column_name": .name,
        "description": .description
      }
  )
'

violations_json="$(
  "$YQ_BIN" eval -o=json -I=0 "$YQ_EXPRESSION" "${FILE_LIST[@]}" \
    | "$JQ_BIN" -cs --argjson limit "$LIMIT" '
        map(
          . + {
            length: (.description | length),
            overflow: ((.description | length) - $limit)
          }
        )
        | map(select(.length > $limit))
      '
)"

violation_count="$("$JQ_BIN" 'length' <<<"$violations_json")"
description_count="$(
  "$YQ_BIN" eval -o=json -I=0 "$YQ_EXPRESSION" "${FILE_LIST[@]}" \
    | "$JQ_BIN" -s 'length'
)"

if [[ "$violation_count" == "0" ]]; then
  echo "Checked ${description_count} column descriptions under ${ROOTS[*]}. No descriptions exceed ${LIMIT} characters."
  exit 0
fi

echo "Found ${violation_count} column description(s) longer than ${LIMIT} characters:"
"$JQ_BIN" -r '
  .[]
  | "\(.file): \(.resource_type)=\(.resource_name // "<unknown>") column=\(.column_name // "<unknown>") length=\(.length) overflow=\(.overflow)"
' <<<"$violations_json"

exit 1
