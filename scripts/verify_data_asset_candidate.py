#!/usr/bin/env python3

"""Resolve and recheck the exact mutable data-asset candidate used by release CI."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence


CANDIDATE_BUCKET = "tuva-public-resources-candidates"
CANDIDATE_CACHE_CONTROL = "no-store,max-age=0,must-revalidate"
CANDIDATE_MARKER = "_candidate.json"
RELEASE_RECEIPT = "_release.json"
GITHUB_API_BASE_URL = "https://api.github.com"
PACKAGE_REPOSITORIES = {"tuva-core": "tuva-health/tuva-core"}
STORE_BASE_URLS = {
    "s3": f"https://{CANDIDATE_BUCKET}.s3.amazonaws.com",
    "gcs": f"https://storage.googleapis.com/{CANDIDATE_BUCKET}",
    "azure": (
        "https://tuvapublicresources.blob.core.windows.net/"
        f"{CANDIDATE_BUCKET}"
    ),
}
RECEIPT_KEYS = {"schema_version", "package", "version", "package_commit", "files"}
FILE_KEYS = {"path", "sha256", "bytes", "rows"}
SEMVER_PATTERN = re.compile(
    r"^[1-9][0-9]*\.[0-9]+\.[0-9]+"
    r"(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$"
)
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class CandidateError(RuntimeError):
    """A candidate cannot be proven safe and current."""


class HttpStatusError(CandidateError):
    """An object-store request returned a non-success HTTP status."""

    def __init__(self, method: str, url: str, status: int) -> None:
        self.method = method
        self.url = url
        self.status = status
        super().__init__(f"{method} {url} returned HTTP {status}.")


@dataclass(frozen=True)
class HttpResult:
    body: bytes
    headers: Mapping[str, str]
    sha256: str | None = None
    bytes_read: int | None = None


Transport = Callable[[str, str], HttpResult]
GithubFetch = Callable[[str], object | None]
ReleaseGuard = Callable[[str, str], None]


def _normalized_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {str(key).lower(): str(value).strip() for key, value in headers.items()}


def _http_request(method: str, url: str) -> HttpResult:
    if method not in {"GET", "HEAD", "HASH"}:
        raise CandidateError(f"Unsupported HTTP method {method!r}.")
    request = urllib.request.Request(
        url,
        method="GET" if method == "HASH" else method,
        headers={
            "Accept-Encoding": "gzip",
            "Cache-Control": "no-cache",
            "User-Agent": "tuva-core-release-candidate-verifier/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            if method == "HASH":
                digest = hashlib.sha256()
                bytes_read = 0
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
                    bytes_read += len(chunk)
                return HttpResult(
                    body=b"",
                    headers=dict(response.headers.items()),
                    sha256=digest.hexdigest(),
                    bytes_read=bytes_read,
                )
            body = response.read() if method == "GET" else b""
            return HttpResult(body=body, headers=dict(response.headers.items()))
    except urllib.error.HTTPError as exc:
        raise HttpStatusError(method, url, exc.code) from exc
    except urllib.error.URLError as exc:
        raise CandidateError(f"{method} {url} failed: {exc.reason}.") from exc


def _github_json_or_none(path: str) -> object | None:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "tuva-core-release-candidate-verifier/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        f"{GITHUB_API_BASE_URL}/{path.lstrip('/')}",
        method="GET",
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise CandidateError(
            f"GitHub release-state check returned HTTP {exc.code}."
        ) from exc
    except (urllib.error.URLError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CandidateError(f"GitHub release-state check failed: {exc}.") from exc


def ensure_version_unreleased(
    package: str,
    version: str,
    *,
    github_fetch: GithubFetch = _github_json_or_none,
) -> None:
    repository = PACKAGE_REPOSITORIES.get(package)
    if repository is None:
        raise CandidateError(f"Unsupported candidate package {package!r}.")
    if not SEMVER_PATTERN.fullmatch(version):
        raise CandidateError("Expected candidate version is not canonical semver.")
    tag = f"v{version}"
    encoded_tag = urllib.parse.quote(tag, safe="")
    if github_fetch(f"repos/{repository}/git/ref/tags/{encoded_tag}") is not None:
        raise CandidateError(f"Package tag already exists; candidate is frozen: {tag}.")
    release = github_fetch(f"repos/{repository}/releases/tags/{encoded_tag}")
    if release is None:
        return
    if not isinstance(release, dict) or not isinstance(release.get("draft"), bool):
        raise CandidateError("GitHub release-state response has an invalid schema.")
    if not release["draft"]:
        raise CandidateError(
            f"Published GitHub release already exists; candidate is frozen: {tag}."
        )


def _object_url(base_url: str, package: str, version: str, path: str) -> str:
    key = "/".join((package, version, path))
    return f"{base_url.rstrip('/')}/{urllib.parse.quote(key, safe='/')}"


def _require_exact_string(value: object, expected: str, label: str) -> None:
    if value != expected:
        raise CandidateError(f"Candidate {label} must equal {expected!r}.")


def _require_nonnegative_integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CandidateError(f"Candidate {label} must be a nonnegative integer.")
    return value


def read_package_version(package_root: Path) -> str:
    text = (package_root / "dbt_project.yml").read_text(encoding="utf-8")
    match = re.search(
        r"(?m)^version: '([1-9][0-9]*\.[0-9]+\.[0-9]+"
        r"(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?)'$",
        text,
    )
    if match is None:
        raise CandidateError("dbt_project.yml does not contain one canonical package version.")
    return match.group(1)


def read_catalog_paths(package_root: Path, expected_package: str) -> tuple[str, ...]:
    text = (package_root / "data_assets.yml").read_text(encoding="utf-8")
    schema_match = re.search(r"(?m)^schema_version: (\d+)$", text)
    package_match = re.search(r"(?m)^package: (\S+)$", text)
    if schema_match is None or int(schema_match.group(1)) != 1:
        raise CandidateError("data_assets.yml must use schema_version 1.")
    if package_match is None or package_match.group(1) != expected_package:
        raise CandidateError("data_assets.yml package does not match the candidate package.")
    paths = tuple(
        re.findall(r"(?m)^  - seed: \S+\n    path: (\S+)$", text)
    )
    if not paths or len(paths) != len(set(paths)):
        raise CandidateError("data_assets.yml paths must be nonempty and unique.")
    return paths


def parse_candidate_marker(
    marker_bytes: bytes,
    *,
    expected_package: str,
    expected_version: str,
    expected_package_commit: str,
    expected_paths: Sequence[str],
) -> dict[str, object]:
    try:
        marker = json.loads(marker_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CandidateError("Candidate marker is not valid UTF-8 JSON.") from exc
    if not isinstance(marker, dict) or set(marker) != RECEIPT_KEYS:
        raise CandidateError("Candidate marker has an unsupported top-level schema.")
    if marker.get("schema_version") != 1:
        raise CandidateError("Candidate marker must use schema_version 1.")
    _require_exact_string(marker.get("package"), expected_package, "package")
    _require_exact_string(marker.get("version"), expected_version, "version")
    _require_exact_string(
        marker.get("package_commit"),
        expected_package_commit,
        "package_commit",
    )
    if not SHA_PATTERN.fullmatch(expected_package_commit):
        raise CandidateError("Expected package commit must be a lowercase 40-character SHA.")

    raw_files = marker.get("files")
    if not isinstance(raw_files, list):
        raise CandidateError("Candidate marker files must be a list.")
    files_by_path: dict[str, dict[str, object]] = {}
    for index, item in enumerate(raw_files):
        if not isinstance(item, dict) or set(item) != FILE_KEYS:
            raise CandidateError(f"Candidate file entry {index} has an invalid schema.")
        path = item.get("path")
        digest = item.get("sha256")
        if (
            not isinstance(path, str)
            or not path
            or path.startswith("/")
            or ".." in path.split("/")
            or path in files_by_path
        ):
            raise CandidateError(f"Candidate file entry {index} has an invalid path.")
        if not isinstance(digest, str) or not SHA256_PATTERN.fullmatch(digest):
            raise CandidateError(f"Candidate file {path} has an invalid SHA-256.")
        _require_nonnegative_integer(item.get("bytes"), f"byte count for {path}")
        _require_nonnegative_integer(item.get("rows"), f"row count for {path}")
        files_by_path[path] = item

    actual_paths = tuple(sorted(files_by_path))
    if actual_paths != tuple(sorted(expected_paths)):
        missing = sorted(set(expected_paths) - set(actual_paths))
        extra = sorted(set(actual_paths) - set(expected_paths))
        raise CandidateError(
            f"Candidate inventory differs from data_assets.yml; missing={missing}, extra={extra}."
        )
    return marker


def _cache_control_is_exact(value: str | None) -> bool:
    if value is None:
        return False
    tokens = {token.strip().lower() for token in value.split(",") if token.strip()}
    return tokens == {"no-store", "max-age=0", "must-revalidate"}


def _verify_headers(
    result: HttpResult,
    *,
    expected_bytes: int,
    expected_content_type: str,
    expected_content_encoding: str | None,
    label: str,
) -> None:
    headers = _normalized_headers(result.headers)
    try:
        actual_bytes = int(headers.get("content-length", ""))
    except ValueError as exc:
        raise CandidateError(f"{label} has an invalid Content-Length.") from exc
    if actual_bytes != expected_bytes:
        raise CandidateError(
            f"{label} Content-Length is {actual_bytes}; expected {expected_bytes}."
        )
    content_type = headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != expected_content_type:
        raise CandidateError(
            f"{label} Content-Type is {content_type!r}; expected {expected_content_type!r}."
        )
    content_encoding = headers.get("content-encoding")
    if content_encoding != expected_content_encoding:
        raise CandidateError(
            f"{label} Content-Encoding is {content_encoding!r}; "
            f"expected {expected_content_encoding!r}."
        )
    if not _cache_control_is_exact(headers.get("cache-control")):
        raise CandidateError(f"{label} must use {CANDIDATE_CACHE_CONTROL!r}.")


def _require_absent(url: str, transport: Transport) -> None:
    try:
        transport("HEAD", url)
    except HttpStatusError as exc:
        if exc.status == 404:
            return
        raise
    raise CandidateError(f"Candidate store contains forbidden {RELEASE_RECEIPT}: {url}.")


def _object_identity(
    store: str,
    result: HttpResult,
    *,
    label: str,
) -> dict[str, str]:
    headers = _normalized_headers(result.headers)
    if store == "s3":
        version_id = headers.get("x-amz-version-id", "")
        if not version_id or version_id.lower() == "null":
            raise CandidateError(f"{label} has no immutable S3 version ID.")
        return {"version_id": version_id}
    if store == "gcs":
        generation = headers.get("x-goog-generation", "")
        metageneration = headers.get("x-goog-metageneration", "")
        if not re.fullmatch(r"[1-9][0-9]*", generation):
            raise CandidateError(f"{label} has no valid GCS generation.")
        if not re.fullmatch(r"[1-9][0-9]*", metageneration):
            raise CandidateError(f"{label} has no valid GCS metageneration.")
        return {
            "generation": generation,
            "metageneration": metageneration,
        }
    if store == "azure":
        etag = headers.get("etag", "")
        if not etag:
            raise CandidateError(f"{label} has no Azure ETag.")
        return {"etag": etag}
    raise CandidateError(f"Unsupported candidate store {store!r}.")


def _validate_locked_identities(
    value: object,
    *,
    expected_paths: Sequence[str],
) -> Mapping[str, object]:
    if not isinstance(value, dict) or set(value) != {"s3", "gcs", "azure"}:
        raise CandidateError("Source lock candidate object identities are invalid.")
    required_paths = {CANDIDATE_MARKER, *expected_paths}
    identity_keys = {
        "s3": {"version_id"},
        "gcs": {"generation", "metageneration"},
        "azure": {"etag"},
    }
    for store, expected_keys in identity_keys.items():
        objects = value.get(store)
        if not isinstance(objects, dict) or set(objects) != required_paths:
            raise CandidateError(
                f"Source lock candidate {store} object inventory is invalid."
            )
        for path, identity in objects.items():
            if not isinstance(identity, dict) or set(identity) != expected_keys:
                raise CandidateError(
                    f"Source lock identity for {store} object {path} is invalid."
                )
            if any(not isinstance(item, str) or not item for item in identity.values()):
                raise CandidateError(
                    f"Source lock identity for {store} object {path} is invalid."
                )
            if store == "s3" and identity["version_id"].lower() == "null":
                raise CandidateError(
                    f"Source lock identity for {store} object {path} is invalid."
                )
            if store == "gcs" and any(
                not re.fullmatch(r"[1-9][0-9]*", identity[key])
                for key in expected_keys
            ):
                raise CandidateError(
                    f"Source lock identity for {store} object {path} is invalid."
                )
    return value


def verify_candidate(
    *,
    expected_package: str,
    expected_version: str,
    expected_package_commit: str,
    expected_paths: Sequence[str],
    expected_marker_sha256: str | None = None,
    expected_object_identities: Mapping[str, object] | None = None,
    full_payload_hashes: bool = False,
    transport: Transport = _http_request,
    store_base_urls: Mapping[str, str] = STORE_BASE_URLS,
) -> tuple[dict[str, object], str, dict[str, dict[str, dict[str, str]]]]:
    if not SEMVER_PATTERN.fullmatch(expected_version):
        raise CandidateError("Expected candidate version is not canonical semver.")
    if set(store_base_urls) != {"s3", "gcs", "azure"}:
        raise CandidateError("Candidate verification requires exactly S3, GCS, and Azure.")

    marker_urls = {
        store: _object_url(base, expected_package, expected_version, CANDIDATE_MARKER)
        for store, base in store_base_urls.items()
    }
    marker_results: dict[str, HttpResult] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(transport, "GET", url): store
            for store, url in marker_urls.items()
        }
        for future in concurrent.futures.as_completed(futures):
            store = futures[future]
            marker_results[store] = future.result()

    baseline = marker_results["s3"].body
    marker_sha256 = hashlib.sha256(baseline).hexdigest()
    if expected_marker_sha256 is not None:
        if (
            not SHA256_PATTERN.fullmatch(expected_marker_sha256)
            or marker_sha256 != expected_marker_sha256
        ):
            raise CandidateError("Candidate marker SHA-256 changed; rerun release CI.")
    for store, result in sorted(marker_results.items()):
        if result.body != baseline:
            raise CandidateError(f"{store} candidate marker bytes differ from S3.")
        _verify_headers(
            result,
            expected_bytes=len(baseline),
            expected_content_type="application/json",
            expected_content_encoding=None,
            label=f"{store} candidate marker",
        )

    object_identities: dict[str, dict[str, dict[str, str]]] = {
        store: {
            CANDIDATE_MARKER: _object_identity(
                store,
                result,
                label=f"{store} candidate marker",
            )
        }
        for store, result in sorted(marker_results.items())
    }

    marker = parse_candidate_marker(
        baseline,
        expected_package=expected_package,
        expected_version=expected_version,
        expected_package_commit=expected_package_commit,
        expected_paths=expected_paths,
    )

    absence_checks = [
        _object_url(base, expected_package, expected_version, RELEASE_RECEIPT)
        for base in store_base_urls.values()
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        list(executor.map(lambda url: _require_absent(url, transport), absence_checks))

    files = {item["path"]: item for item in marker["files"]}  # type: ignore[index]
    checks: list[tuple[str, str, str, int, str, str]] = []
    for store, base in sorted(store_base_urls.items()):
        for path in expected_paths:
            checks.append(
                (
                    store,
                    path,
                    _object_url(base, expected_package, expected_version, path),
                    int(files[path]["bytes"]),
                    str(files[path]["sha256"]),
                    f"{store} candidate payload {path}",
                )
            )

    def check_payload(
        check: tuple[str, str, str, int, str, str]
    ) -> tuple[str, str, dict[str, str]]:
        store, path, url, byte_count, expected_sha256, label = check
        result = transport("HASH" if full_payload_hashes else "HEAD", url)
        _verify_headers(
            result,
            expected_bytes=byte_count,
            expected_content_type="text/csv",
            expected_content_encoding="gzip",
            label=label,
        )
        if full_payload_hashes:
            if result.bytes_read != byte_count:
                raise CandidateError(
                    f"{label} yielded {result.bytes_read} bytes; expected {byte_count}."
                )
            if result.sha256 != expected_sha256:
                raise CandidateError(f"{label} SHA-256 differs from the marker.")
        return store, path, _object_identity(store, result, label=label)

    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
        checked_payloads = list(executor.map(check_payload, checks))
    for store, path, identity in checked_payloads:
        object_identities[store][path] = identity

    if (
        expected_object_identities is not None
        and object_identities != expected_object_identities
    ):
        raise CandidateError(
            "Candidate object identity changed in S3, GCS, or Azure; rerun release CI."
        )
    return marker, marker_sha256, object_identities


def _candidate_lock(
    marker: Mapping[str, object],
    marker_sha256: str,
    object_identities: Mapping[str, object],
) -> dict[str, object]:
    files = marker["files"]
    assert isinstance(files, list)
    return {
        "bucket": CANDIDATE_BUCKET,
        "file_count": len(files),
        "marker_sha256": marker_sha256,
        "object_identities": object_identities,
        "package": marker["package"],
        "package_commit": marker["package_commit"],
        "stores": ["s3", "gcs", "azure"],
        "version": marker["version"],
    }


def resolve_source_lock(
    *,
    package_root: Path,
    package: str,
    package_commit: str,
    source_lock: Mapping[str, object],
    transport: Transport = _http_request,
    store_base_urls: Mapping[str, str] = STORE_BASE_URLS,
    release_guard: ReleaseGuard = ensure_version_unreleased,
) -> dict[str, object]:
    if "data_asset_candidate" in source_lock:
        raise CandidateError("Source lock already contains a data-asset candidate.")
    version = read_package_version(package_root)
    paths = read_catalog_paths(package_root, package)
    release_guard(package, version)
    marker, marker_sha256, object_identities = verify_candidate(
        expected_package=package,
        expected_version=version,
        expected_package_commit=package_commit,
        expected_paths=paths,
        full_payload_hashes=True,
        transport=transport,
        store_base_urls=store_base_urls,
    )
    verify_candidate(
        expected_package=package,
        expected_version=version,
        expected_package_commit=package_commit,
        expected_paths=paths,
        expected_marker_sha256=marker_sha256,
        expected_object_identities=object_identities,
        transport=transport,
        store_base_urls=store_base_urls,
    )
    release_guard(package, version)
    resolved = dict(source_lock)
    resolved["data_asset_candidate"] = _candidate_lock(
        marker,
        marker_sha256,
        object_identities,
    )
    return resolved


def verify_source_lock_candidate(
    *,
    package_root: Path,
    source_lock: Mapping[str, object],
    transport: Transport = _http_request,
    store_base_urls: Mapping[str, str] = STORE_BASE_URLS,
    require_unreleased: bool = True,
    release_guard: ReleaseGuard = ensure_version_unreleased,
) -> None:
    candidate = source_lock.get("data_asset_candidate")
    required_keys = {
        "bucket",
        "file_count",
        "marker_sha256",
        "object_identities",
        "package",
        "package_commit",
        "stores",
        "version",
    }
    if not isinstance(candidate, dict) or set(candidate) != required_keys:
        raise CandidateError("Source lock has an invalid data-asset candidate.")
    package = candidate.get("package")
    version = candidate.get("version")
    package_commit = candidate.get("package_commit")
    marker_sha256 = candidate.get("marker_sha256")
    if not all(isinstance(value, str) for value in (
        package,
        version,
        package_commit,
        marker_sha256,
    )):
        raise CandidateError("Source lock candidate identifiers must be strings.")
    if candidate.get("bucket") != CANDIDATE_BUCKET:
        raise CandidateError("Source lock candidate bucket is invalid.")
    if candidate.get("stores") != ["s3", "gcs", "azure"]:
        raise CandidateError("Source lock candidate stores are invalid.")
    if version != read_package_version(package_root):
        raise CandidateError("Source lock candidate version differs from the checkout.")
    paths = read_catalog_paths(package_root, package)
    if candidate.get("file_count") != len(paths):
        raise CandidateError("Source lock candidate file count differs from data_assets.yml.")
    object_identities = _validate_locked_identities(
        candidate.get("object_identities"),
        expected_paths=paths,
    )
    if require_unreleased:
        release_guard(package, version)
    verify_candidate(
        expected_package=package,
        expected_version=version,
        expected_package_commit=package_commit,
        expected_paths=paths,
        expected_marker_sha256=marker_sha256,
        expected_object_identities=object_identities,
        transport=transport,
        store_base_urls=store_base_urls,
    )
    if require_unreleased:
        release_guard(package, version)


def _load_json_object(raw: str, label: str) -> dict[str, object]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CandidateError(f"{label} is not valid JSON.") from exc
    if not isinstance(value, dict):
        raise CandidateError(f"{label} must be a JSON object.")
    return value


def _write_github_output(path: Path, name: str, value: str) -> None:
    if "\n" in value or "\r" in value:
        raise CandidateError(f"GitHub output {name} must be one line.")
    with path.open("a", encoding="utf-8") as output:
        output.write(f"{name}={value}\n")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    resolve = subparsers.add_parser(
        "resolve", description="Validate all candidate stores and extend a source lock."
    )
    resolve.add_argument("--package-root", default=".")
    resolve.add_argument("--package", default="tuva-core")
    resolve.add_argument("--package-commit", required=True)
    resolve.add_argument("--source-lock-json", required=True)
    resolve.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT"))

    verify = subparsers.add_parser(
        "verify", description="Recheck the source-locked candidate in all clouds."
    )
    verify.add_argument("--package-root", default=".")
    verify.add_argument("--source-lock-json", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        package_root = Path(args.package_root).resolve()
        source_lock = _load_json_object(args.source_lock_json, "source lock")
        if args.command == "resolve":
            resolved = resolve_source_lock(
                package_root=package_root,
                package=args.package,
                package_commit=args.package_commit,
                source_lock=source_lock,
            )
            compact = json.dumps(resolved, separators=(",", ":"), sort_keys=True)
            if args.github_output:
                output_path = Path(args.github_output)
                _write_github_output(output_path, "source_lock", compact)
                candidate = resolved["data_asset_candidate"]
                assert isinstance(candidate, dict)
                _write_github_output(
                    output_path,
                    "candidate_marker_sha256",
                    str(candidate["marker_sha256"]),
                )
                _write_github_output(
                    output_path,
                    "package_version",
                    str(candidate["version"]),
                )
            else:
                print(compact)
            return 0
        verify_source_lock_candidate(
            package_root=package_root,
            source_lock=source_lock,
        )
        print("verified source-locked data-asset candidate in S3, GCS, and Azure")
        return 0
    except (CandidateError, FileNotFoundError, OSError) as exc:
        print(f"candidate verification failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
