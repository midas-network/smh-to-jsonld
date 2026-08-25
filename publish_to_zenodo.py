#!/usr/bin/env python3
"""Package SMH data and publish it to a Zenodo Sandbox deposition.

The safe default creates an unpublished draft. Pass ``--publish`` together
with ``--acknowledge-rights-reviewed`` to publish the draft and mint a Sandbox
DOI. The access token is read only from an environment variable.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import subprocess
import sys
import tarfile
import tempfile
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import requests
import yaml

from utils.jsonld import DEFAULT_UNKNOWN_LICENSE, normalize_license

REPOSITORY_ROOT = Path(__file__).resolve().parent
DEFAULT_API_URL = "https://sandbox.zenodo.org/api"
DEFAULT_TOKEN_ENV = "ZENODO_SANDBOX_TOKEN"
DEFAULT_RELEASE_DIR = REPOSITORY_ROOT / "release" / "zenodo"
NATIVE_JSON_MEDIA_TYPE = "application/vnd.inveniordm.v1+json"
SOURCE_REPOSITORY = "https://github.com/midas-network/rsv-scenario-modeling-hub"
PARSER_REPOSITORY = "https://github.com/midas-network/smh-to-jsonld"
PARSER_RELEASE_TAG = "v0.1.0-beta.1"
PARSER_RELEASE_URL = f"{PARSER_REPOSITORY}/releases/tag/{PARSER_RELEASE_TAG}"
DATASET_COPYRIGHT = (
    "Copyright, where applicable, is retained by the respective Scenario "
    "Modeling Hub model teams and authors. Reuse terms vary by model-round; "
    "see LICENSES.json."
)
COORDINATION_FUNDING = (
    "The Scenario Modeling Hub is supported by the MIDAS Coordination Center, "
    "NIGMS grants U24GM132013 (2019–2024) and R24GM153920 (2024–2029) to "
    "the University of Pittsburgh."
)
RSV_MESH_SUBJECT = {
    "term": "Respiratory Syncytial Virus Infections",
    "identifier": "http://id.nlm.nih.gov/mesh/D018357",
    "scheme": "url",
}
OBSOLETE_RELEASE_FILES = {"rsv-smh-generated-output.tar.gz"}
LICENSE_IDS = {
    "bsd simplified": "bsd-2-clause",
    "cc-by-4.0": "cc-by-4.0",
    "cc-by-sa-4.0": "cc-by-sa-4.0",
    "cc-by_sa-4.0": "cc-by-sa-4.0",
    "mit": "mit",
    DEFAULT_UNKNOWN_LICENSE: DEFAULT_UNKNOWN_LICENSE,
}
LICENSE_URLS = {
    "bsd-2-clause": "https://opensource.org/license/bsd-2-clause",
    "cc-by-4.0": "https://creativecommons.org/licenses/by/4.0/",
    "cc-by-sa-4.0": "https://creativecommons.org/licenses/by-sa/4.0/",
    "mit": "https://opensource.org/license/mit/",
    DEFAULT_UNKNOWN_LICENSE: (
        f"https://zenodo.org/api/vocabularies/licenses/{DEFAULT_UNKNOWN_LICENSE}"
    ),
}
RECORD_LICENSE_IDS = (
    "cc-by-4.0",
    "cc-by-sa-4.0",
    "mit",
    "bsd-2-clause",
    DEFAULT_UNKNOWN_LICENSE,
)


class ZenodoError(RuntimeError):
    """An error that is safe to show without exposing the access token."""


@dataclass(frozen=True)
class UploadedFile:
    name: str
    size: int
    checksum: str | None


class ZenodoClient:
    """Small client for the parts of the Zenodo deposition API that we use."""

    def __init__(
        self,
        token: str,
        *,
        api_url: str = DEFAULT_API_URL,
        timeout: float = 300.0,
        session: requests.Session | None = None,
    ) -> None:
        if not token.strip():
            raise ValueError("The Zenodo access token is empty.")

        self.api_url = api_url.rstrip("/")
        parsed_url = urlparse(self.api_url)
        if parsed_url.scheme != "https" or not parsed_url.hostname:
            raise ValueError("The Zenodo API URL must be an HTTPS URL.")

        self._allowed_hostname = parsed_url.hostname
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "User-Agent": "smh-parser-zenodo-publisher/1.0",
            }
        )

    def create_deposition(self, metadata: dict[str, Any]) -> dict[str, Any]:
        response = self.session.post(
            f"{self.api_url}/deposit/depositions",
            json={"metadata": metadata},
            timeout=self.timeout,
        )
        return self._json_response(response, {201}, "create the draft deposition")

    def get_deposition(self, deposition_id: int) -> dict[str, Any]:
        response = self.session.get(
            f"{self.api_url}/deposit/depositions/{deposition_id}",
            timeout=self.timeout,
        )
        return self._json_response(response, {200}, "retrieve the draft deposition")

    def update_deposition(
        self, deposition_id: int, metadata: dict[str, Any]
    ) -> dict[str, Any]:
        response = self.session.put(
            f"{self.api_url}/deposit/depositions/{deposition_id}",
            json={"metadata": metadata},
            timeout=self.timeout,
        )
        return self._json_response(response, {200}, "update the draft metadata")

    def enrich_draft_metadata(
        self,
        deposition_id: int,
        *,
        rights: Iterable[str],
        copyright_statement: str | None,
    ) -> dict[str, Any]:
        """Set native fields that Zenodo's legacy deposition API drops."""
        headers = {
            "Accept": NATIVE_JSON_MEDIA_TYPE,
            "Content-Type": "application/json",
        }
        draft_url = f"{self.api_url}/records/{deposition_id}/draft"
        response = self.session.get(
            draft_url,
            headers=headers,
            timeout=self.timeout,
        )
        draft = self._json_response(
            response, {200}, "retrieve the native draft metadata"
        )
        native_metadata = draft.get("metadata")
        if not isinstance(native_metadata, dict):
            raise ZenodoError("Zenodo did not return native metadata for the draft.")

        license_ids = tuple(dict.fromkeys(value for value in rights if value))
        if license_ids:
            native_metadata["rights"] = [
                {"id": license_id} for license_id in license_ids
            ]
        if copyright_statement:
            native_metadata["copyright"] = copyright_statement

        files = draft.get("files") or {}
        payload = {
            "metadata": native_metadata,
            "access": draft.get("access") or {},
            "files": {"enabled": bool(files.get("enabled", True))},
            "custom_fields": draft.get("custom_fields") or {},
        }
        response = self.session.put(
            draft_url,
            headers=headers,
            json=payload,
            timeout=self.timeout,
        )
        return self._json_response(
            response, {200}, "update the native draft metadata"
        )

    def upload_file(self, bucket_url: str, path: Path) -> UploadedFile:
        parsed_bucket = urlparse(bucket_url)
        if (
            parsed_bucket.scheme != "https"
            or parsed_bucket.hostname != self._allowed_hostname
        ):
            raise ZenodoError(
                "Zenodo returned an unexpected upload host; refusing to send the token."
            )

        print(
            f"Uploading {path.name} ({_human_size(path.stat().st_size)})...",
            file=sys.stderr,
        )
        upload_url = f"{bucket_url.rstrip('/')}/{quote(path.name, safe='')}"
        with path.open("rb") as stream:
            response = self.session.put(
                upload_url,
                data=stream,
                timeout=self.timeout,
            )
        payload = self._json_response(response, {200, 201}, f"upload {path.name}")
        remote_checksum = payload.get("checksum")
        _verify_zenodo_md5(path, remote_checksum)
        return UploadedFile(
            name=payload.get("key") or payload.get("filename") or path.name,
            size=int(
                payload.get("size") or payload.get("filesize") or path.stat().st_size
            ),
            checksum=remote_checksum,
        )

    def publish_deposition(self, deposition_id: int) -> dict[str, Any]:
        response = self.session.post(
            f"{self.api_url}/deposit/depositions/{deposition_id}/actions/publish",
            timeout=self.timeout,
        )
        return self._json_response(response, {202}, "publish the deposition")

    def delete_deposition(self, deposition_id: int) -> None:
        response = self.session.delete(
            f"{self.api_url}/deposit/depositions/{deposition_id}",
            timeout=self.timeout,
        )
        if response.status_code not in {201, 204}:
            raise self._response_error(response, "delete the incomplete draft")

    def delete_deposition_file(self, deposition_id: int, file_id: str) -> None:
        response = self.session.delete(
            f"{self.api_url}/deposit/depositions/{deposition_id}/files/{file_id}",
            timeout=self.timeout,
        )
        if response.status_code != 204:
            raise self._response_error(response, "delete the obsolete draft file")

    @staticmethod
    def _json_response(
        response: requests.Response, expected: set[int], operation: str
    ) -> dict[str, Any]:
        if response.status_code not in expected:
            raise ZenodoClient._response_error(response, operation)
        try:
            payload = response.json()
        except ValueError as exc:
            raise ZenodoError(
                f"Zenodo returned a non-JSON response while trying to {operation}."
            ) from exc
        if not isinstance(payload, dict):
            raise ZenodoError(
                f"Zenodo returned an unexpected response while trying to {operation}."
            )
        return payload

    @staticmethod
    def _response_error(response: requests.Response, operation: str) -> ZenodoError:
        detail: Any = response.text.strip()
        try:
            payload = response.json()
            detail = payload.get("message") or payload.get("errors") or detail
        except (ValueError, AttributeError):
            pass
        if not isinstance(detail, str):
            detail = json.dumps(detail, ensure_ascii=False)
        detail = detail[:500] if detail else "No error detail was returned."
        return ZenodoError(
            f"Could not {operation} (HTTP {response.status_code}): {detail}"
        )


def build_release(
    *,
    repository_root: Path,
    release_dir: Path,
    rounds: Iterable[str],
    include_source_data: bool = True,
    include_generated_output: bool = True,
    reuse_archives: bool = False,
) -> list[Path]:
    """Create self-contained round archives, rights metadata, and checksums."""
    selected_rounds = tuple(rounds)
    release_dir.mkdir(parents=True, exist_ok=True)
    artifacts: list[Path] = []

    if not include_source_data and not include_generated_output:
        raise ValueError(
            "At least one of source data or generated output must be included."
        )

    consolidated_outputs = _find_consolidated_outputs(repository_root, selected_rounds)
    for round_id in selected_rounds:
        entries: list[tuple[Path, str]] = []
        if include_source_data:
            source = repository_root / "data" / round_id
            if not source.is_dir():
                raise ValueError(f"Round data directory does not exist: {source}")
            entries.append((source, f"data/{round_id}"))
        if include_generated_output:
            round_output = repository_root / "output" / round_id
            if not round_output.is_dir():
                raise ValueError(
                    f"Generated round output directory does not exist: {round_output}"
                )
            entries.append((round_output, f"output/{round_id}"))
            for output in consolidated_outputs[round_id]:
                entries.append((output, f"output/{output.name}"))

        destination = release_dir / f"rsv-smh-{round_id}.tar.gz"
        _create_archive(
            entries,
            destination,
            reuse=reuse_archives,
            required_prefixes=[archive_name for _, archive_name in entries],
        )
        artifacts.append(destination)

    # This was used by the first prototype. Generated files now live with each round.
    (release_dir / "rsv-smh-generated-output.tar.gz").unlink(missing_ok=True)

    license_manifest = build_license_manifest(repository_root, selected_rounds)
    source_snapshots = _load_source_snapshots(repository_root, selected_rounds)

    readme_path = release_dir / "README.md"
    readme_path.write_text(
        _build_release_readme(selected_rounds, license_manifest, source_snapshots),
        encoding="utf-8",
    )

    licenses_path = release_dir / "LICENSES.json"
    licenses_path.write_text(
        json.dumps(license_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    provenance_path = release_dir / "release_metadata.json"
    provenance = {
        "generated_at": datetime.now(UTC).isoformat(),
        "parser_repository": PARSER_REPOSITORY,
        "parser_release": PARSER_RELEASE_TAG,
        "parser_release_url": PARSER_RELEASE_URL,
        "parser_commit": _git_value(repository_root, "rev-parse", "HEAD"),
        "parser_worktree_dirty": bool(
            _git_value(repository_root, "status", "--porcelain", "--untracked-files=no")
        ),
        "source_repository": SOURCE_REPOSITORY,
        "source_snapshots": source_snapshots,
        "rounds": list(selected_rounds),
        "artifacts": [
            {
                "name": path.name,
                "bytes": path.stat().st_size,
                "sha256": _file_digest(path, "sha256"),
            }
            for path in artifacts
        ],
    }
    provenance_path.write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    checksum_inputs = [*artifacts, readme_path, licenses_path, provenance_path]
    checksum_path = release_dir / "SHA256SUMS"
    checksum_path.write_text(
        "".join(
            f"{_file_digest(path, 'sha256')}  {path.name}\n" for path in checksum_inputs
        ),
        encoding="utf-8",
    )
    return [*checksum_inputs, checksum_path]


def build_license_manifest(
    repository_root: Path, rounds: Iterable[str]
) -> dict[str, Any]:
    """Build a model-level license manifest from the source YAML metadata."""
    models: list[dict[str, Any]] = []
    for round_id in rounds:
        metadata_dir = repository_root / "data" / round_id / "model-metadata"
        if not metadata_dir.is_dir():
            raise ValueError(f"Model metadata directory does not exist: {metadata_dir}")
        for path in sorted((*metadata_dir.glob("*.yaml"), *metadata_dir.glob("*.yml"))):
            if path.name.casefold() == "readme.md":
                continue
            raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            declared = raw.get("license")
            effective = _zenodo_license_id(normalize_license(declared))
            model_id = path.stem
            models.append(
                {
                    "round_id": round_id,
                    "model_id": model_id,
                    "declared_license": declared,
                    "effective_license": effective,
                    "license_url": LICENSE_URLS.get(effective),
                    "fallback_applied": effective == DEFAULT_UNKNOWN_LICENSE,
                    "source_metadata": f"data/{round_id}/model-metadata/{path.name}",
                    "covered_paths": [
                        f"data/{round_id}/model-output/{model_id}/",
                        f"output/{round_id}/{model_id}.jsonld",
                    ],
                }
            )

    license_counts: dict[str, int] = {}
    for model in models:
        license_id = model["effective_license"]
        license_counts[license_id] = license_counts.get(license_id, 0) + 1

    record_level_licenses = [
        license_id for license_id in RECORD_LICENSE_IDS if license_id in license_counts
    ]
    record_level_licenses.extend(
        sorted(set(license_counts).difference(record_level_licenses))
    )
    return {
        "schema_version": "1.0",
        "record_level_licenses": record_level_licenses,
        "license_counts": dict(sorted(license_counts.items())),
        "unknown_license_fallback": DEFAULT_UNKNOWN_LICENSE,
        "unknown_license_policy": (
            "Free for private use; the right holder retains other rights, "
            "including distribution."
        ),
        "models": models,
    }


def _load_source_snapshots(
    repository_root: Path, rounds: Iterable[str]
) -> list[dict[str, Any]]:
    """Load the exact upstream tag and commit recorded during data retrieval."""
    snapshots: list[dict[str, Any]] = []
    required = ("repository", "ref", "ref_type", "commit", "retrieved_at")
    for round_id in rounds:
        path = repository_root / "data" / round_id / "source_snapshot.json"
        if not path.is_file():
            raise ValueError(
                f"Source provenance is missing for {round_id}; rerun "
                "pipeline/update_source_data.py for that round."
            )
        snapshot = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(snapshot, dict) or any(
            not snapshot.get(field) for field in required
        ):
            raise ValueError(f"Source provenance is incomplete: {path}")
        snapshots.append({"round_id": round_id, **snapshot})
    return snapshots


def _build_release_readme(
    rounds: tuple[str, ...],
    license_manifest: dict[str, Any],
    source_snapshots: list[dict[str, Any]],
) -> str:
    """Build the human-readable guide uploaded alongside the round archives."""
    archive_rows = "\n".join(
        f"| `{round_id}` | `rsv-smh-{round_id}.tar.gz` |"
        for round_id in rounds
    )
    provenance_rows = "\n".join(
        f"| `{item['round_id']}` | `{item['ref']}` | `{item['commit']}` |"
        for item in source_snapshots
    )
    license_rows = "\n".join(
        f"| `{license_id}` | {count} |"
        for license_id, count in license_manifest["license_counts"].items()
    )
    model_count = len(license_manifest["models"])
    return f"""# RSV Scenario Modeling Hub data, 2023–2026

This release contains conditional long-term respiratory syncytial virus (RSV)
scenario projections submitted to the Scenario Modeling Hub. These projections
compare outcomes under specified intervention and epidemiological scenarios;
they are not unconditional forecasts of what will happen.

The release contains {model_count} model-round datasets across {len(rounds)}
scenario rounds. Outputs cover United States national and participating
RSV-NET state locations, weekly hospitalization targets, age groups, scenarios,
sample trajectories, and—where submitted—quantiles and derived targets.

## Files

| Round origin | Self-contained archive |
| --- | --- |
{archive_rows}

Each archive preserves the following layout:

- `data/<round>/hub-config/`: Hubverse task and validation configuration.
- `data/<round>/model-metadata/`: submitting-team model metadata.
- `data/<round>/model-output/`: Apache Parquet scenario projections.
- `data/<round>/source_snapshot.json`: exact upstream tag and commit.
- `output/<round>/`: generated per-model Schema.org JSON-LD.
- `output/*.jsonld` and `output/*.html`: consolidated machine- and
  human-readable round metadata.

The files beside the archives are:

- `LICENSES.json`: model-round license assignments and fallback decisions.
- `release_metadata.json`: parser and source provenance plus artifact hashes.
- `SHA256SUMS`: SHA-256 checksums for release files.

## Source provenance

The source repository is {SOURCE_REPOSITORY}. The copied snapshots are:

| Round origin | Source tag | Source commit |
| --- | --- | --- |
{provenance_rows}

Generated metadata was produced with
[`{PARSER_RELEASE_TAG}`]({PARSER_RELEASE_URL}) of `smh-to-jsonld`. See
`release_metadata.json` for the exact parser commit used to build this upload.

## Targets and interpretation

The principal target is weekly incident RSV hospital admissions. Depending on
the round and submission, the data may also contain cumulative hospitalizations,
peak timing, peak magnitude, infections, quantile summaries, and paired sample
trajectories. Scenario IDs, age groups, locations, horizons, and output types
are defined in each archive's `hub-config/tasks.json` and model metadata.

Weeks, scenario assumptions, calibration cutoffs, and projection intervals are
round-specific. Consult the consolidated HTML/JSON-LD and the linked Scenario
Modeling Hub round documentation before analysis.

## Licensing

The upload is mixed-license. `LICENSES.json` is authoritative for assigning
terms to each model-round dataset; no single license applies to every file.

| Effective license | Model-round datasets |
| --- | ---: |
{license_rows}

Missing or unknown declarations use `{DEFAULT_UNKNOWN_LICENSE}` as a
conservative fallback: free for private use, while the right holder retains
other rights, including distribution. The Apache-2.0 license for the parser
software does not relicense these research data.

{DATASET_COPYRIGHT}

## Citation and contact

Cite the version-specific DOI shown on the Zenodo record for the exact release
used in an analysis. Preserve model-team attribution and the license mapping
when redistributing permitted subsets. Questions about the Hub or this release
may be sent to `questions@midasnetwork.us`.
"""


def upload_release(
    client: ZenodoClient,
    files: Iterable[Path],
    *,
    metadata: dict[str, Any],
    publish: bool,
    keep_failed_draft: bool = False,
    record_license_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    paths = tuple(path.resolve() for path in files)
    _validate_upload_files(paths)
    deposition = client.create_deposition(metadata)
    try:
        deposition_id = int(deposition["id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ZenodoError("Zenodo did not return an ID for the new draft.") from exc
    links = deposition.get("links", {})

    try:
        if metadata.get("copyright") or record_license_ids is not None:
            client.enrich_draft_metadata(
                deposition_id,
                rights=(
                    record_license_ids
                    if record_license_ids is not None
                    else (str(metadata.get("license") or ""),)
                ),
                copyright_statement=metadata.get("copyright"),
            )
        bucket_url = links.get("bucket")
        if not isinstance(bucket_url, str):
            raise ZenodoError("Zenodo did not return a bucket URL for the new draft.")
        uploaded = [client.upload_file(bucket_url, path) for path in paths]
    except (OSError, requests.RequestException, ValueError, ZenodoError):
        if not keep_failed_draft:
            try:
                client.delete_deposition(deposition_id)
            except (
                OSError,
                requests.RequestException,
                ValueError,
                ZenodoError,
            ) as cleanup_error:
                print(
                    f"Warning: draft {deposition_id} could not be cleaned up: {cleanup_error}",
                    file=sys.stderr,
                )
        raise

    published_record: dict[str, Any] | None = None
    if publish:
        # A publish failure intentionally leaves the complete draft available for review.
        try:
            published_record = client.publish_deposition(deposition_id)
        except (OSError, requests.RequestException, ValueError, ZenodoError):
            print(
                f"Draft {deposition_id} is complete but could not be published; "
                "it remains available for review.",
                file=sys.stderr,
            )
            raise

    final_record = published_record or deposition
    final_links = final_record.get("links", {})
    return {
        "deposition_id": deposition_id,
        "published": publish,
        "doi": final_record.get("doi"),
        "url": (
            final_links.get("record_html")
            or final_links.get("html")
            or links.get("html")
            or links.get("self")
        ),
        "files": [asdict(item) for item in uploaded],
    }


def publish_existing_draft(client: ZenodoClient, deposition_id: int) -> dict[str, Any]:
    record = client.publish_deposition(deposition_id)
    links = record.get("links", {})
    return {
        "deposition_id": deposition_id,
        "published": True,
        "doi": record.get("doi"),
        "url": links.get("record_html") or links.get("html") or links.get("self"),
    }


def update_existing_draft(
    client: ZenodoClient,
    deposition_id: int,
    files: Iterable[Path],
    *,
    metadata: dict[str, Any],
    record_license_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Upload the desired release, remove known obsolete files, then update metadata."""
    paths = tuple(path.resolve() for path in files)
    _validate_upload_files(paths)
    deposition = client.get_deposition(deposition_id)
    if deposition.get("submitted"):
        raise ZenodoError(f"Deposition {deposition_id} is already published.")
    links = deposition.get("links", {})
    bucket_url = links.get("bucket")
    if not isinstance(bucket_url, str):
        raise ZenodoError("Zenodo did not return a bucket URL for the draft.")

    # Validate and apply metadata before touching files. If this fails, the draft's
    # existing file set is left unchanged.
    updated = client.update_deposition(deposition_id, metadata)
    if metadata.get("copyright") or record_license_ids is not None:
        client.enrich_draft_metadata(
            deposition_id,
            rights=(
                record_license_ids
                if record_license_ids is not None
                else (str(metadata.get("license") or ""),)
            ),
            copyright_statement=metadata.get("copyright"),
        )
    remote_by_name = {
        str(item.get("filename") or item.get("name")): item
        for item in deposition.get("files", [])
    }
    unchanged: list[str] = []
    pending_uploads: list[Path] = []
    for path in paths:
        remote = remote_by_name.get(path.name)
        if remote and _md5_matches(path, remote.get("checksum")):
            unchanged.append(path.name)
        else:
            pending_uploads.append(path)
    uploaded = [client.upload_file(bucket_url, path) for path in pending_uploads]
    refreshed = client.get_deposition(deposition_id)
    removed: list[str] = []
    for remote_file in refreshed.get("files", []):
        filename = remote_file.get("filename") or remote_file.get("name")
        file_id = remote_file.get("id")
        if filename in OBSOLETE_RELEASE_FILES and file_id:
            client.delete_deposition_file(deposition_id, str(file_id))
            removed.append(str(filename))

    updated_links = updated.get("links", {})
    return {
        "deposition_id": deposition_id,
        "published": False,
        "url": updated_links.get("html") or links.get("html") or links.get("self"),
        "files": [asdict(item) for item in uploaded],
        "unchanged_files": sorted(unchanged),
        "removed_files": sorted(removed),
        "metadata": updated.get("metadata"),
    }


def build_metadata(
    args: argparse.Namespace,
    rounds: Iterable[str],
    repository_root: Path = REPOSITORY_ROOT,
) -> dict[str, Any]:
    selected_rounds = tuple(rounds)
    collections = _load_round_collections(repository_root, selected_rounds)
    internal = _extract_discovery_metadata(collections)
    creators = (
        [{"name": name} for name in args.creator]
        if args.creator
        else [
            {
                "name": "Scenario Modeling Hub Coordination Group",
                "affiliation": "MIDAS Network",
            }
        ]
    )
    round_labels = ", ".join(
        f"{_display_round_name(item['name'])} (origin {item['round_id']})"
        for item in internal["rounds"]
    )
    description = args.description or (
        "<p>This dataset contains conditional long-term respiratory syncytial "
        "virus (RSV) scenario projections submitted to the Scenario Modeling "
        "Hub, together with model metadata and generated JSON-LD and HTML "
        "representations. The scenarios compare possible trajectories under "
        "specified assumptions; they are not unconditional forecasts.</p>"
        f"<p>It covers {len(internal['rounds'])} rounds and "
        f"{internal['model_round_count']} model-round datasets: {round_labels}.</p>"
        "<p>Each round archive is self-contained and includes Hubverse "
        "configuration, model metadata, Apache Parquet model outputs, per-model "
        "JSON-LD, and consolidated round-level JSON-LD and HTML. Outputs cover "
        "United States national and participating RSV-NET state locations, "
        "weekly hospitalization targets, age groups, scenarios, sample "
        "trajectories, and—where submitted—quantiles and derived targets.</p>"
    )
    subjects = [RSV_MESH_SUBJECT, *internal["subjects"]]
    metadata: dict[str, Any] = {
        "title": args.title,
        "upload_type": "dataset",
        "description": description,
        "copyright": DATASET_COPYRIGHT,
        "access_right": "open",
        "license": args.license,
        "creators": creators,
        "contributors": internal["contributors"],
        "version": args.version,
        "keywords": [
            "respiratory syncytial virus",
            "Respiratory Syncytial Virus Infections",
            "RSV",
            "RSV-NET",
            "scenario modeling",
            "scenario projections",
            "conditional projections",
            "RSV hospitalization",
            "weekly incident hospital admissions",
            "age-stratified projections",
            "state-level projections",
            "probabilistic trajectories",
            "ensemble modeling",
            "infectious disease modeling",
            "Hubverse",
            "Apache Parquet",
            "JSON-LD",
            "United States",
            *selected_rounds,
        ],
        "subjects": sorted(subjects, key=lambda item: item["term"]),
        "dates": internal["dates"],
        "language": "eng",
        "method": (
            "Source snapshots are obtained from release-tagged rounds of the RSV "
            "Scenario Modeling Hub. Each data/{round}/source_snapshot.json records "
            "the exact upstream tag and commit. The smh-to-jsonld pipeline preserves "
            "Hubverse configuration, model metadata, and Parquet projections; "
            "derives per-model and consolidated Schema.org JSON-LD; and renders HTML "
            "views. release_metadata.json records source snapshots, the parser "
            "release and commit, and artifact checksums."
        ),
        "notes": (
            "<p>This is a mixed-license upload. LICENSES.json records the declared "
            "and effective license for each model-round dataset. Missing or unknown "
            "declarations use "
            f"<code>{DEFAULT_UNKNOWN_LICENSE}</code>: free for private use; the "
            "right holder retains other rights, including distribution.</p>"
            f"<p>{COORDINATION_FUNDING}</p>"
            f"<p>Funding statements preserved from model metadata:</p>"
            f"<ul>{''.join(f'<li>{statement}</li>' for statement in internal['funding'])}</ul>"
        ),
        "related_identifiers": [
            {
                "identifier": PARSER_REPOSITORY,
                "relation": "isCompiledBy",
                "resource_type": "software",
            },
            {
                "identifier": PARSER_RELEASE_URL,
                "relation": "isCompiledBy",
                "resource_type": "software",
            },
            {
                "identifier": SOURCE_REPOSITORY,
                "relation": "isDerivedFrom",
                "resource_type": "dataset",
            },
            *[
                {
                    "identifier": url,
                    "relation": "isDocumentedBy",
                    "resource_type": "publication-other",
                }
                for url in internal["documentation_urls"]
            ],
        ],
    }

    if args.metadata_file:
        custom = json.loads(args.metadata_file.read_text(encoding="utf-8"))
        if not isinstance(custom, dict):
            raise ValueError("The metadata file must contain a JSON object.")
        if isinstance(custom.get("metadata"), dict):
            custom = custom["metadata"]
        metadata.update(custom)

    if args.community:
        metadata["communities"] = [
            {"identifier": identifier} for identifier in args.community
        ]

    required = ("title", "upload_type", "description", "creators")
    missing = [field for field in required if not metadata.get(field)]
    if missing:
        raise ValueError(f"Required Zenodo metadata is missing: {', '.join(missing)}")
    return metadata


def _load_round_collections(
    repository_root: Path, rounds: Iterable[str]
) -> list[dict[str, Any]]:
    outputs = _find_consolidated_outputs(repository_root, tuple(rounds))
    collections: list[dict[str, Any]] = []
    for round_id, paths in outputs.items():
        jsonld_paths = [path for path in paths if path.suffix == ".jsonld"]
        if len(jsonld_paths) != 1:
            raise ValueError(
                f"Expected one consolidated JSON-LD file for {round_id}; "
                f"found {len(jsonld_paths)}."
            )
        data = json.loads(jsonld_paths[0].read_text(encoding="utf-8"))
        collections.append(data)
    return collections


def _extract_discovery_metadata(
    collections: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    people: dict[str, dict[str, Any]] = {}
    organizations: set[str] = set()
    subjects: dict[str, dict[str, str]] = {}
    dates: dict[tuple[str, str, str], dict[str, str]] = {}
    funding: set[str] = set()
    documentation_urls: set[str] = set()
    rounds: list[dict[str, str]] = []
    model_round_count = 0

    for collection in collections:
        round_id = str(collection.get("roundId") or collection.get("identifier"))
        round_name = str(collection.get("name") or f"Round {round_id}")
        rounds.append({"round_id": round_id, "name": round_name})
        parts = collection.get("hasPart") or []
        model_round_count += len(parts)
        for part in parts:
            producer = part.get("producer") or {}
            producer_name = _clean_text(producer.get("name"))
            if producer_name and "coordination group" not in producer_name.casefold():
                organizations.add(producer_name)
            funder = producer.get("funder") or {}
            if isinstance(funder, dict):
                statement = _clean_text(funder.get("description"))
                if statement:
                    funding.add(statement)

            for author in part.get("author") or []:
                if not isinstance(author, dict):
                    continue
                name = _clean_text(author.get("name"))
                if not name or "coordination group" in name.casefold():
                    continue
                key = " ".join(name.casefold().split())
                person = people.setdefault(key, {"name": name, "affiliations": set()})
                affiliation = author.get("affiliation") or {}
                if isinstance(affiliation, dict):
                    affiliation = affiliation.get("name")
                clean_affiliation = _clean_text(affiliation)
                if clean_affiliation:
                    person["affiliations"].update(
                        value
                        for part in clean_affiliation.split(";")
                        if (value := _clean_text(part))
                    )

            work = part.get("workExample") or {}
            for variable in work.get("variableMeasured") or []:
                if not isinstance(variable, dict):
                    continue
                term = _clean_text(variable.get("name"))
                identifier = _clean_text(variable.get("identifier"))
                if term and identifier:
                    subjects[identifier] = {
                        "term": term,
                        "identifier": identifier,
                        "scheme": "url",
                    }

            coverage = _clean_text(work.get("temporalCoverage"))
            if coverage and "/" in coverage:
                start, end = (_iso_date(value) for value in coverage.split("/", 1))
                dates[(start, end, round_id)] = {
                    "start": start,
                    "end": end,
                    "type": "Valid",
                    "description": _display_round_name(round_name),
                }

            event = work.get("isPartOf") or {}
            subject_of = event.get("subjectOf") or {}
            url = _clean_text(subject_of.get("url") or event.get("url"))
            if url:
                documentation_urls.add(url)

    contributors = [
        {
            "name": person["name"],
            "type": "Researcher",
            **(
                {"affiliation": "; ".join(sorted(person["affiliations"]))}
                if person["affiliations"]
                else {}
            ),
        }
        for person in sorted(people.values(), key=lambda item: item["name"].casefold())
    ]
    contributors.extend(
        {"name": name, "type": "ResearchGroup"}
        for name in sorted(organizations, key=str.casefold)
    )
    return {
        "contributors": contributors,
        "subjects": sorted(subjects.values(), key=lambda item: item["term"]),
        "dates": sorted(dates.values(), key=lambda item: item["start"]),
        "funding": sorted(funding),
        "documentation_urls": sorted(documentation_urls),
        "rounds": sorted(rounds, key=lambda item: item["round_id"]),
        "model_round_count": model_round_count,
    }


def discover_rounds(repository_root: Path) -> tuple[str, ...]:
    data_dir = repository_root / "data"
    if not data_dir.is_dir():
        return ()
    return tuple(
        path.name
        for path in sorted(data_dir.iterdir())
        if path.is_dir() and not path.name.startswith(".")
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Package SMH data and upload it to Zenodo Sandbox."
    )
    parser.add_argument(
        "--round",
        action="append",
        dest="rounds",
        help="Round directory to include; repeat as needed. Defaults to every data round.",
    )
    parser.add_argument(
        "--release-dir",
        type=Path,
        default=DEFAULT_RELEASE_DIR,
        help=f"Archive output directory (default: {DEFAULT_RELEASE_DIR}).",
    )
    parser.add_argument(
        "--reuse-archives",
        action="store_true",
        help="Reuse existing non-empty archives instead of rebuilding them.",
    )
    parser.add_argument(
        "--skip-source-data",
        action="store_true",
        help="Do not package the source Parquet/configuration data.",
    )
    parser.add_argument(
        "--skip-generated-output",
        action="store_true",
        help="Do not package the generated JSON-LD and HTML output.",
    )
    parser.add_argument(
        "--title",
        default="RSV Scenario Modeling Hub data and JSON-LD metadata, 2023–2026",
    )
    parser.add_argument(
        "--description",
        help="HTML-capable description. Defaults to one generated from internal metadata.",
    )
    parser.add_argument(
        "--creator",
        action="append",
        help="Creator name; repeat to preserve Zenodo creator order.",
    )
    parser.add_argument(
        "--version", default=datetime.now(UTC).date().strftime("%Y.%m.%d")
    )
    parser.add_argument(
        "--license",
        default=DEFAULT_UNKNOWN_LICENSE,
        help=(
            "Record-level Zenodo license identifier "
            f"(default: {DEFAULT_UNKNOWN_LICENSE})."
        ),
    )
    parser.add_argument(
        "--metadata-file",
        type=Path,
        help="JSON object whose values override the generated Zenodo metadata.",
    )
    parser.add_argument(
        "--community",
        action="append",
        help="Zenodo community identifier; repeat as needed (e.g. midas-network).",
    )
    parser.add_argument(
        "--token-env",
        default=DEFAULT_TOKEN_ENV,
        help=f"Environment variable holding the token (default: {DEFAULT_TOKEN_ENV}).",
    )
    parser.add_argument(
        "--api-url",
        default=os.getenv("ZENODO_API_URL", DEFAULT_API_URL),
        help="Zenodo API URL. Defaults to Zenodo Sandbox.",
    )
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and describe release files without contacting Zenodo.",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Create, upload, and publish in one run. Otherwise a draft is created.",
    )
    parser.add_argument(
        "--publish-draft",
        type=int,
        metavar="DEPOSITION_ID",
        help="Publish an existing reviewed draft without repackaging or uploading.",
    )
    parser.add_argument(
        "--update-draft",
        type=int,
        metavar="DEPOSITION_ID",
        help="Replace release files and metadata in an existing unpublished draft.",
    )
    parser.add_argument(
        "--acknowledge-rights-reviewed",
        action="store_true",
        help="Confirm model-specific redistribution rights were reviewed before publishing.",
    )
    parser.add_argument(
        "--keep-failed-draft",
        action="store_true",
        help="Keep a newly created draft if an upload fails.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    selected_actions = sum(
        bool(action) for action in (args.publish, args.publish_draft, args.update_draft)
    )
    if selected_actions > 1:
        parser.error("Use only one of --publish, --publish-draft, or --update-draft.")
    if args.dry_run and args.publish_draft:
        parser.error("--dry-run cannot be combined with --publish-draft.")
    if (args.publish or args.publish_draft) and not args.acknowledge_rights_reviewed:
        parser.error(
            "--publish and --publish-draft require --acknowledge-rights-reviewed."
        )
    if not args.dry_run and not os.getenv(args.token_env):
        parser.error(f"Set {args.token_env} before contacting Zenodo Sandbox.")

    if args.publish_draft:
        try:
            client = ZenodoClient(
                os.environ[args.token_env],
                api_url=args.api_url,
                timeout=args.timeout,
            )
            result = publish_existing_draft(client, args.publish_draft)
        except (OSError, requests.RequestException, ValueError, ZenodoError) as exc:
            print(f"Zenodo release failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result, indent=2))
        print("Sandbox deposition published.", file=sys.stderr)
        return 0

    rounds = tuple(args.rounds or discover_rounds(REPOSITORY_ROOT))
    if not rounds:
        parser.error("No data round directories were found.")

    try:
        metadata = build_metadata(args, rounds, REPOSITORY_ROOT)
        record_license_ids = build_license_manifest(REPOSITORY_ROOT, rounds)[
            "record_level_licenses"
        ]
        release_files = build_release(
            repository_root=REPOSITORY_ROOT,
            release_dir=args.release_dir.resolve(),
            rounds=rounds,
            include_source_data=not args.skip_source_data,
            include_generated_output=not args.skip_generated_output,
            reuse_archives=args.reuse_archives,
        )

        if args.dry_run:
            result = {
                "dry_run": True,
                "would_publish": args.publish,
                "would_update_draft": args.update_draft,
                "rounds": list(rounds),
                "metadata": metadata,
                "files": [
                    {
                        "path": str(path),
                        "bytes": path.stat().st_size,
                        "sha256": _file_digest(path, "sha256"),
                    }
                    for path in release_files
                ],
            }
        else:
            token = os.environ[args.token_env]
            client = ZenodoClient(token, api_url=args.api_url, timeout=args.timeout)
            if args.update_draft:
                result = update_existing_draft(
                    client,
                    args.update_draft,
                    release_files,
                    metadata=metadata,
                    record_license_ids=record_license_ids,
                )
            else:
                result = upload_release(
                    client,
                    release_files,
                    metadata=metadata,
                    publish=args.publish,
                    keep_failed_draft=args.keep_failed_draft,
                    record_license_ids=record_license_ids,
                )
    except (OSError, requests.RequestException, ValueError, ZenodoError) as exc:
        print(f"Zenodo release failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2))
    if args.dry_run:
        print("Dry run complete; Zenodo was not contacted.", file=sys.stderr)
    elif args.publish:
        print("Sandbox deposition published.", file=sys.stderr)
    elif args.update_draft:
        print(
            "Sandbox draft files and metadata updated; it remains unpublished.",
            file=sys.stderr,
        )
    else:
        print("Sandbox draft created; it has not been published.", file=sys.stderr)
    return 0


def _create_archive(
    entries: Iterable[tuple[Path, str]],
    destination: Path,
    *,
    reuse: bool,
    required_prefixes: Iterable[str],
) -> None:
    archive_entries = tuple(entries)
    if reuse and _archive_contains(destination, required_prefixes):
        print(
            f"Reusing {destination.name} "
            f"({_human_size(destination.stat().st_size)})...",
            file=sys.stderr,
        )
        return

    print(
        f"Building {destination.name} from {len(archive_entries)} source paths...",
        file=sys.stderr,
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=destination.parent, prefix=f".{destination.name}.", delete=False
        ) as raw_stream:
            temporary_name = raw_stream.name
            with (
                gzip.GzipFile(
                    filename="", mode="wb", fileobj=raw_stream, mtime=0
                ) as gzip_stream,
                tarfile.open(fileobj=gzip_stream, mode="w") as archive,
            ):
                for source, archive_name in archive_entries:
                    archive.add(
                        source,
                        arcname=archive_name,
                        recursive=True,
                        filter=_archive_filter,
                    )
        os.replace(temporary_name, destination)
    finally:
        if temporary_name:
            Path(temporary_name).unlink(missing_ok=True)


def _archive_contains(destination: Path, required_prefixes: Iterable[str]) -> bool:
    if not destination.is_file() or destination.stat().st_size == 0:
        return False
    try:
        with tarfile.open(destination, "r:gz") as archive:
            names = set(archive.getnames())
    except (OSError, tarfile.TarError):
        return False
    return all(
        prefix in names or any(name.startswith(f"{prefix}/") for name in names)
        for prefix in required_prefixes
    )


def _archive_filter(info: tarfile.TarInfo) -> tarfile.TarInfo | None:
    if Path(info.name).name == ".DS_Store":
        return None
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    return info


def _find_consolidated_outputs(
    repository_root: Path, rounds: tuple[str, ...]
) -> dict[str, list[Path]]:
    wanted = set(rounds)
    outputs: dict[str, list[Path]] = {round_id: [] for round_id in rounds}
    output_dir = repository_root / "output"
    if not output_dir.is_dir():
        raise ValueError(f"Generated output directory does not exist: {output_dir}")
    for path in sorted(output_dir.glob("*.jsonld")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        round_id = str(payload.get("roundId") or payload.get("identifier"))
        if round_id not in wanted or not isinstance(payload.get("hasPart"), list):
            continue
        outputs[round_id].append(path)
        html_path = path.with_suffix(".html")
        if html_path.is_file():
            outputs[round_id].append(html_path)
    missing = [round_id for round_id, paths in outputs.items() if not paths]
    if missing:
        raise ValueError(
            "No consolidated generated output found for round(s): " + ", ".join(missing)
        )
    return outputs


def _zenodo_license_id(value: str) -> str:
    normalized = value.strip().casefold()
    return LICENSE_IDS.get(normalized, value.strip())


def _display_round_name(value: Any) -> str:
    """Use season-oriented labels when every annual round is internally Round 1."""
    name = _clean_text(value)
    if name.casefold().startswith("round 1 - "):
        name = name[len("Round 1 - ") :]
    suffix = " Scenario Projection Models Collection"
    name = name.removesuffix(suffix)
    return name or "Scenario round"


def _clean_text(value: Any) -> str:
    return " ".join(str(value).split()) if value is not None else ""


def _iso_date(value: str) -> str:
    return value.strip().split(" ", 1)[0]


def _validate_upload_files(paths: tuple[Path, ...]) -> None:
    if not paths:
        raise ValueError("At least one release file is required.")
    names: set[str] = set()
    for path in paths:
        if not path.is_file():
            raise ValueError(f"Release file does not exist: {path}")
        if path.name in names:
            raise ValueError(f"Duplicate upload filename: {path.name}")
        names.add(path.name)


def _verify_zenodo_md5(path: Path, remote_checksum: Any) -> None:
    if not isinstance(remote_checksum, str) or not remote_checksum.startswith("md5:"):
        return
    expected = remote_checksum.removeprefix("md5:")
    if _file_digest(path, "md5") != expected:
        raise ZenodoError(f"Checksum verification failed for {path.name}.")


def _md5_matches(path: Path, remote_checksum: Any) -> bool:
    if not isinstance(remote_checksum, str):
        return False
    expected = remote_checksum.removeprefix("md5:")
    return _file_digest(path, "md5") == expected


def _file_digest(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_value(repository_root: Path, *arguments: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(repository_root), *arguments],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


def _human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
