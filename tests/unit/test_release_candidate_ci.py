#!/usr/bin/env python3

import hashlib
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "scripts" / "verify_data_asset_candidate.py"
SPEC = importlib.util.spec_from_file_location("verify_data_asset_candidate", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class FakeTransport:
    def __init__(self, responses):
        self.responses = dict(responses)
        self.calls = []

    def __call__(self, method, url):
        self.calls.append((method, url))
        value = self.responses.get((method, url))
        if value is None:
            raise MODULE.HttpStatusError(method, url, 404)
        if isinstance(value, Exception):
            raise value
        return value


class ReleaseCandidateCiTest(unittest.TestCase):
    package = "tuva-core"
    version = "1.0.0"
    package_commit = "a" * 40
    paths = (
        "terminology/terminology__example.csv.gz",
        "provider-data/provider_data__example.csv.gz",
    )
    bases = {
        "s3": "https://s3.example",
        "gcs": "https://gcs.example",
        "azure": "https://azure.example",
    }
    payloads = {
        paths[0]: b"terminology-bytes",
        paths[1]: b"provider-data-payload-bytes",
    }

    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.package_root = Path(self.temporary_directory.name)
        (self.package_root / "dbt_project.yml").write_text(
            "name: 'the_tuva_project'\nversion: '1.0.0'\n",
            encoding="utf-8",
        )
        (self.package_root / "data_assets.yml").write_text(
            "\n".join(
                [
                    "schema_version: 1",
                    "package: tuva-core",
                    "assets:",
                    "  - seed: terminology__example",
                    f"    path: {self.paths[0]}",
                    "  - seed: provider_data__example",
                    f"    path: {self.paths[1]}",
                    "",
                ]
            ),
            encoding="utf-8",
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    @staticmethod
    def allow_unreleased(_package, _version):
        return None

    def marker(self, *, commit=None):
        return {
            "schema_version": 1,
            "package": self.package,
            "version": self.version,
            "package_commit": commit or self.package_commit,
            "files": [
                {
                    "path": path,
                    "sha256": hashlib.sha256(self.payloads[path]).hexdigest(),
                    "bytes": len(self.payloads[path]),
                    "rows": index + 2,
                }
                for index, path in enumerate(self.paths)
            ],
        }

    def identity_headers(self, store, path):
        index = (MODULE.CANDIDATE_MARKER, *self.paths).index(path) + 1
        if store == "s3":
            return {"x-amz-version-id": f"s3-version-{index}"}
        if store == "gcs":
            return {
                "x-goog-generation": str(1000 + index),
                "x-goog-metageneration": "1",
            }
        if store == "azure":
            return {"ETag": f'"azure-etag-{index}"'}
        raise AssertionError(f"unexpected store: {store}")

    def headers(
        self,
        store,
        path,
        byte_count,
        content_type,
        content_encoding=None,
    ):
        headers = {
            "Content-Length": str(byte_count),
            "Content-Type": content_type,
            "Cache-Control": " must-revalidate, MAX-AGE=0 , no-store ",
            **self.identity_headers(store, path),
        }
        if content_encoding is not None:
            headers["Content-Encoding"] = content_encoding
        return headers

    def transport(self, marker=None):
        marker = marker or self.marker()
        marker_bytes = json.dumps(marker, sort_keys=True).encode()
        responses = {}
        for store, base in self.bases.items():
            marker_url = MODULE._object_url(
                base, self.package, self.version, MODULE.CANDIDATE_MARKER
            )
            responses[("GET", marker_url)] = MODULE.HttpResult(
                marker_bytes,
                self.headers(
                    store,
                    MODULE.CANDIDATE_MARKER,
                    len(marker_bytes),
                    "application/json; charset=utf-8",
                ),
            )
            for item in marker["files"]:
                path = item["path"]
                payload_url = MODULE._object_url(
                    base, self.package, self.version, path
                )
                payload_headers = self.headers(
                    store,
                    path,
                    item["bytes"],
                    "text/csv",
                    "gzip",
                )
                responses[("HASH", payload_url)] = MODULE.HttpResult(
                    b"",
                    payload_headers,
                    sha256=hashlib.sha256(self.payloads[path]).hexdigest(),
                    bytes_read=len(self.payloads[path]),
                )
                responses[("HEAD", payload_url)] = MODULE.HttpResult(
                    b"",
                    payload_headers,
                )
        return FakeTransport(responses), marker_bytes

    def resolve(self, transport):
        return MODULE.resolve_source_lock(
            package_root=self.package_root,
            package=self.package,
            package_commit=self.package_commit,
            source_lock={
                "core": {"revision": "d" * 40},
                "standalone_packages": [],
            },
            transport=transport,
            store_base_urls=self.bases,
            release_guard=self.allow_unreleased,
        )

    def test_resolve_hashes_and_pins_every_object_in_all_three_clouds(self):
        transport, marker_bytes = self.transport()
        resolved = self.resolve(transport)

        candidate = resolved["data_asset_candidate"]
        self.assertEqual(candidate["bucket"], MODULE.CANDIDATE_BUCKET)
        self.assertEqual(candidate["file_count"], 2)
        self.assertEqual(candidate["package_commit"], self.package_commit)
        self.assertEqual(
            candidate["marker_sha256"], hashlib.sha256(marker_bytes).hexdigest()
        )
        self.assertEqual(candidate["stores"], ["s3", "gcs", "azure"])
        expected_objects = {MODULE.CANDIDATE_MARKER, *self.paths}
        for store in self.bases:
            self.assertEqual(
                set(candidate["object_identities"][store]), expected_objects
            )

        payload_hash_calls = [
            call for call in transport.calls if call[0] == "HASH"
        ]
        payload_head_calls = [
            call
            for call in transport.calls
            if call[0] == "HEAD" and call[1].endswith(".csv.gz")
        ]
        self.assertEqual(len(payload_hash_calls), len(self.paths) * 3)
        self.assertEqual(len(payload_head_calls), len(self.paths) * 3)

    def test_recheck_uses_heads_and_never_redownloads_payloads(self):
        transport, _ = self.transport()
        source_lock = self.resolve(transport)
        transport.calls.clear()

        MODULE.verify_source_lock_candidate(
            package_root=self.package_root,
            source_lock=source_lock,
            transport=transport,
            store_base_urls=self.bases,
            release_guard=self.allow_unreleased,
        )

        self.assertFalse(any(method == "HASH" for method, _url in transport.calls))
        payload_head_calls = [
            call
            for call in transport.calls
            if call[0] == "HEAD" and call[1].endswith(".csv.gz")
        ]
        self.assertEqual(len(payload_head_calls), len(self.paths) * 3)

    def test_different_cloud_marker_fails_closed(self):
        transport, _ = self.transport()
        gcs_url = MODULE._object_url(
            self.bases["gcs"], self.package, self.version, MODULE.CANDIDATE_MARKER
        )
        original = transport.responses[("GET", gcs_url)]
        transport.responses[("GET", gcs_url)] = MODULE.HttpResult(
            original.body + b" ",
            self.headers(
                "gcs",
                MODULE.CANDIDATE_MARKER,
                len(original.body) + 1,
                "application/json",
            ),
        )
        with self.assertRaisesRegex(MODULE.CandidateError, "differ from S3"):
            self.resolve(transport)

    def test_payload_metadata_mismatch_fails_closed(self):
        transport, _ = self.transport()
        url = MODULE._object_url(
            self.bases["azure"], self.package, self.version, self.paths[1]
        )
        original = transport.responses[("HASH", url)]
        bad_headers = dict(original.headers)
        bad_headers["Content-Length"] = str(len(self.payloads[self.paths[1]]) - 1)
        transport.responses[("HASH", url)] = MODULE.HttpResult(
            b"",
            bad_headers,
            sha256=original.sha256,
            bytes_read=original.bytes_read,
        )
        with self.assertRaisesRegex(MODULE.CandidateError, "Content-Length"):
            self.resolve(transport)

    def test_same_length_payload_with_wrong_bytes_fails_closed(self):
        transport, _ = self.transport()
        url = MODULE._object_url(
            self.bases["gcs"], self.package, self.version, self.paths[0]
        )
        original = transport.responses[("HASH", url)]
        transport.responses[("HASH", url)] = MODULE.HttpResult(
            b"",
            original.headers,
            sha256="f" * 64,
            bytes_read=original.bytes_read,
        )
        with self.assertRaisesRegex(MODULE.CandidateError, "SHA-256 differs"):
            self.resolve(transport)

    def test_candidate_must_be_bound_to_pull_request_head(self):
        transport, _ = self.transport(self.marker(commit="e" * 40))
        with self.assertRaisesRegex(MODULE.CandidateError, "package_commit"):
            self.resolve(transport)

    def test_candidate_store_must_not_contain_release_receipt(self):
        transport, _ = self.transport()
        release_url = MODULE._object_url(
            self.bases["s3"], self.package, self.version, MODULE.RELEASE_RECEIPT
        )
        transport.responses[("HEAD", release_url)] = MODULE.HttpResult(
            b"",
            {
                "Content-Length": "10",
                "Content-Type": "application/json",
            },
        )
        with self.assertRaisesRegex(MODULE.CandidateError, "forbidden _release.json"):
            self.resolve(transport)

    def test_release_receipt_403_is_not_treated_as_absent(self):
        transport, _ = self.transport()
        release_url = MODULE._object_url(
            self.bases["s3"], self.package, self.version, MODULE.RELEASE_RECEIPT
        )
        transport.responses[("HEAD", release_url)] = MODULE.HttpStatusError(
            "HEAD", release_url, 403
        )
        with self.assertRaisesRegex(MODULE.CandidateError, "HTTP 403"):
            self.resolve(transport)

    def test_cache_control_requires_exact_writer_token_set(self):
        transport, _ = self.transport()
        marker_url = MODULE._object_url(
            self.bases["s3"], self.package, self.version, MODULE.CANDIDATE_MARKER
        )
        original = transport.responses[("GET", marker_url)]
        bad_headers = dict(original.headers)
        bad_headers["Cache-Control"] = "no-store,max-age=0"
        transport.responses[("GET", marker_url)] = MODULE.HttpResult(
            original.body, bad_headers
        )
        with self.assertRaisesRegex(MODULE.CandidateError, "must use"):
            self.resolve(transport)

    def test_payload_identity_drift_fails_recheck(self):
        transport, _ = self.transport()
        source_lock = self.resolve(transport)
        url = MODULE._object_url(
            self.bases["s3"], self.package, self.version, self.paths[0]
        )
        original = transport.responses[("HEAD", url)]
        changed_headers = dict(original.headers)
        changed_headers["x-amz-version-id"] = "replacement-version"
        transport.responses[("HEAD", url)] = MODULE.HttpResult(b"", changed_headers)

        with self.assertRaisesRegex(MODULE.CandidateError, "object identity changed"):
            MODULE.verify_source_lock_candidate(
                package_root=self.package_root,
                source_lock=source_lock,
                transport=transport,
                store_base_urls=self.bases,
                release_guard=self.allow_unreleased,
            )

    def test_marker_identity_drift_with_same_bytes_fails_recheck(self):
        transport, _ = self.transport()
        source_lock = self.resolve(transport)
        marker_url = MODULE._object_url(
            self.bases["azure"], self.package, self.version, MODULE.CANDIDATE_MARKER
        )
        original = transport.responses[("GET", marker_url)]
        changed_headers = dict(original.headers)
        changed_headers["ETag"] = '"replacement-etag"'
        transport.responses[("GET", marker_url)] = MODULE.HttpResult(
            original.body, changed_headers
        )

        with self.assertRaisesRegex(MODULE.CandidateError, "object identity changed"):
            MODULE.verify_source_lock_candidate(
                package_root=self.package_root,
                source_lock=source_lock,
                transport=transport,
                store_base_urls=self.bases,
                release_guard=self.allow_unreleased,
            )

    def test_source_lock_requires_exact_object_identity_inventory(self):
        transport, _ = self.transport()
        source_lock = self.resolve(transport)
        del source_lock["data_asset_candidate"]["object_identities"]["gcs"][
            self.paths[1]
        ]
        with self.assertRaisesRegex(MODULE.CandidateError, "inventory is invalid"):
            MODULE.verify_source_lock_candidate(
                package_root=self.package_root,
                source_lock=source_lock,
                transport=transport,
                store_base_urls=self.bases,
                release_guard=self.allow_unreleased,
            )

    def test_exact_tag_blocks_candidate_but_draft_release_does_not(self):
        tag_path = "repos/tuva-health/tuva-core/git/ref/tags/v1.0.0"
        release_path = "repos/tuva-health/tuva-core/releases/tags/v1.0.0"

        with self.assertRaisesRegex(MODULE.CandidateError, "tag already exists"):
            MODULE.ensure_version_unreleased(
                self.package,
                self.version,
                github_fetch=lambda path: {"ref": "refs/tags/v1.0.0"}
                if path == tag_path
                else None,
            )

        MODULE.ensure_version_unreleased(
            self.package,
            self.version,
            github_fetch=lambda path: {"draft": True}
            if path == release_path
            else None,
        )

    def test_published_release_blocks_candidate(self):
        release_path = "repos/tuva-health/tuva-core/releases/tags/v1.0.0"
        with self.assertRaisesRegex(MODULE.CandidateError, "Published GitHub release"):
            MODULE.ensure_version_unreleased(
                self.package,
                self.version,
                github_fetch=lambda path: {"draft": False}
                if path == release_path
                else None,
            )

    def test_release_appearing_after_resolution_blocks_recheck(self):
        transport, _ = self.transport()
        calls = 0

        def evolving_guard(_package, _version):
            nonlocal calls
            calls += 1
            if calls > 2:
                raise MODULE.CandidateError(
                    "Published GitHub release already exists; candidate is frozen."
                )

        source_lock = MODULE.resolve_source_lock(
            package_root=self.package_root,
            package=self.package,
            package_commit=self.package_commit,
            source_lock={},
            transport=transport,
            store_base_urls=self.bases,
            release_guard=evolving_guard,
        )
        with self.assertRaisesRegex(MODULE.CandidateError, "candidate is frozen"):
            MODULE.verify_source_lock_candidate(
                package_root=self.package_root,
                source_lock=source_lock,
                transport=transport,
                store_base_urls=self.bases,
                release_guard=evolving_guard,
            )


if __name__ == "__main__":
    unittest.main()
