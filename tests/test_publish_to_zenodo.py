import hashlib
import json
import tarfile
from unittest.mock import Mock

import pytest
import yaml

from publish_to_zenodo import (
    ZenodoClient,
    ZenodoError,
    build_metadata,
    build_parser,
    build_release,
    publish_existing_draft,
    update_existing_draft,
    upload_release,
)


class FakeResponse:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("not JSON")
        return self._payload


def make_client(session):
    session.headers = {}
    return ZenodoClient("not-a-real-token", session=session)


def make_repository(root):
    round_id = "2025-07-27"
    round_dir = root / "data" / round_id
    metadata_dir = round_dir / "model-metadata"
    model_output_dir = round_dir / "model-output" / "NIH-RSV"
    generated_round_dir = root / "output" / round_id
    metadata_dir.mkdir(parents=True)
    model_output_dir.mkdir(parents=True)
    generated_round_dir.mkdir(parents=True)
    (model_output_dir / "model.parquet").write_bytes(b"parquet-test-data")
    (round_dir / ".DS_Store").write_bytes(b"excluded")
    (round_dir / "source_snapshot.json").write_text(
        json.dumps(
            {
                "repository": "https://example.org/source",
                "ref": "2025-07-27-v3",
                "ref_type": "tag",
                "commit": "0123456789abcdef0123456789abcdef01234567",
                "retrieved_at": "2026-08-24T12:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    (metadata_dir / "NIH-RSV.yaml").write_text(
        yaml.safe_dump({"license": "NA"}), encoding="utf-8"
    )
    (generated_round_dir / "NIH-RSV.jsonld").write_text("{}\n", encoding="utf-8")
    consolidated = {
        "@type": "Dataset",
        "roundId": round_id,
        "identifier": round_id,
        "name": "Round 1 - 2025-2026 Scenario Projection Models Collection",
        "hasPart": [
            {
                "name": "NIH-RSV",
                "producer": {
                    "name": "National Institutes of Health",
                    "funder": {"description": "NIH test grant"},
                },
                "author": [
                    {
                        "name": "Doe, Jane",
                        "affiliation": {"name": "NIH"},
                    },
                    {
                        "name": "Doe, Jane",
                        "affiliation": {"name": "Research Center"},
                    },
                ],
                "workExample": {
                    "temporalCoverage": "2025-07-27 00:00:00/2026-06-06 00:00:00",
                    "variableMeasured": [
                        {
                            "name": "Weekly incident RSV hospitalizations",
                            "identifier": "http://example.org/inc-hosp",
                        }
                    ],
                    "isPartOf": {"subjectOf": {"url": "https://example.org/round-3"}},
                },
            }
        ],
    }
    consolidated_path = root / "output" / "Round_1_2025-2026_v6.0.0.jsonld"
    consolidated_path.write_text(json.dumps(consolidated), encoding="utf-8")
    consolidated_path.with_suffix(".html").write_text("<html></html>", encoding="utf-8")
    return round_id


def test_build_release_packages_rounds_outputs_and_checksums(tmp_path):
    repository = tmp_path / "repository"
    round_id = make_repository(repository)

    files = build_release(
        repository_root=repository,
        release_dir=tmp_path / "release",
        rounds=[round_id],
    )

    assert [path.name for path in files] == [
        "rsv-smh-2025-07-27.tar.gz",
        "Round_1_2025-2026_v6.0.0.jsonld",
        "Round_1_2025-2026_v6.0.0.html",
        "README.md",
        "LICENSES.json",
        "release_metadata.json",
        "SHA256SUMS",
    ]

    # The consolidated metadata is uploaded outside the archive too, so it can be
    # fetched directly instead of only existing inside a tarball.
    loose_jsonld = files[1]
    assert loose_jsonld.parent == tmp_path / "release"
    assert json.loads(loose_jsonld.read_text(encoding="utf-8"))["roundId"] == round_id
    assert (
        loose_jsonld.read_bytes()
        == (repository / "output" / "Round_1_2025-2026_v6.0.0.jsonld").read_bytes()
    )

    with tarfile.open(files[0], "r:gz") as archive:
        names = archive.getnames()
        assert "data/2025-07-27/model-output/NIH-RSV/model.parquet" in names
        assert "output/2025-07-27/NIH-RSV.jsonld" in names
        assert "output/Round_1_2025-2026_v6.0.0.jsonld" in names
        assert not any(name.endswith(".DS_Store") for name in archive.getnames())

    checksum_lines = files[-1].read_text(encoding="utf-8").splitlines()
    assert len(checksum_lines) == 6
    assert any(line.endswith("  Round_1_2025-2026_v6.0.0.jsonld") for line in checksum_lines)
    metadata = json.loads(files[-2].read_text(encoding="utf-8"))
    assert metadata["rounds"] == ["2025-07-27"]
    assert metadata["source_snapshots"][0]["ref"] == "2025-07-27-v3"
    assert metadata["parser_release"] == "v0.1.0-beta.1"
    assert (
        metadata["artifacts"][0]["sha256"]
        == hashlib.sha256(files[0].read_bytes()).hexdigest()
    )
    by_name = {path.name: path for path in files}
    release_readme = by_name["README.md"].read_text(encoding="utf-8")
    assert "not unconditional forecasts" in release_readme
    assert "2025-07-27-v3" in release_readme
    licenses = json.loads(by_name["LICENSES.json"].read_text(encoding="utf-8"))
    assert licenses["models"][0]["effective_license"] == "zenodo-freetoread-1.0"
    assert licenses["models"][0]["fallback_applied"] is True
    assert licenses["license_counts"] == {"zenodo-freetoread-1.0": 1}
    assert licenses["record_level_licenses"] == ["zenodo-freetoread-1.0"]


def test_build_metadata_uses_internal_discovery_fields(tmp_path):
    repository = tmp_path / "repository"
    round_id = make_repository(repository)
    args = build_parser().parse_args([])

    metadata = build_metadata(args, [round_id], repository)

    assert metadata["license"] == "zenodo-freetoread-1.0"
    assert "respective Scenario Modeling Hub model teams" in metadata["copyright"]
    assert metadata["creators"] == [
        {
            "name": "Scenario Modeling Hub Coordination Group",
            "affiliation": "MIDAS Network",
        }
    ]
    researcher = next(
        contributor
        for contributor in metadata["contributors"]
        if contributor["type"] == "Researcher"
    )
    assert researcher == {
        "name": "Doe, Jane",
        "type": "Researcher",
        "affiliation": "NIH; Research Center",
    }
    assert metadata["dates"][0]["start"] == "2025-07-27"
    assert any(
        subject["identifier"] == "http://example.org/inc-hosp"
        for subject in metadata["subjects"]
    )
    assert any(
        subject["identifier"] == "http://id.nlm.nih.gov/mesh/D018357"
        for subject in metadata["subjects"]
    )
    assert any(
        related["identifier"].endswith("/releases/tag/v0.1.0-beta.1")
        for related in metadata["related_identifiers"]
    )
    assert "data/{round}/source_snapshot.json" in metadata["method"]


def test_client_enriches_native_rights_and_copyright():
    session = Mock()
    session.headers = {}
    session.get.return_value = FakeResponse(
        200,
        {
            "metadata": {"title": "Dataset", "rights": [{"id": "old"}]},
            "access": {"record": "public", "files": "public"},
            "files": {"enabled": True},
            "custom_fields": {"legacy:subjects": []},
        },
    )
    session.put.return_value = FakeResponse(200, {"id": "46"})

    make_client(session).enrich_draft_metadata(
        46,
        rights=["cc-by-4.0", "mit", "cc-by-4.0"],
        copyright_statement="Copyright retained by the respective authors.",
    )

    request = session.put.call_args
    assert request.args[0].endswith("/records/46/draft")
    assert request.kwargs["headers"]["Accept"] == (
        "application/vnd.inveniordm.v1+json"
    )
    assert request.kwargs["json"]["metadata"]["rights"] == [
        {"id": "cc-by-4.0"},
        {"id": "mit"},
    ]
    assert request.kwargs["json"]["metadata"]["copyright"].startswith("Copyright")


def test_upload_release_creates_draft_without_publishing(tmp_path):
    release_file = tmp_path / "release.tar.gz"
    release_file.write_bytes(b"release")
    checksum = hashlib.md5(release_file.read_bytes()).hexdigest()
    session = Mock()
    session.headers = {}
    session.post.return_value = FakeResponse(
        201,
        {
            "id": 42,
            "links": {
                "bucket": "https://sandbox.zenodo.org/api/files/bucket-id",
                "html": "https://sandbox.zenodo.org/deposit/42",
            },
        },
    )
    session.put.return_value = FakeResponse(
        200,
        {
            "key": release_file.name,
            "size": release_file.stat().st_size,
            "checksum": f"md5:{checksum}",
        },
    )

    result = upload_release(
        make_client(session),
        [release_file],
        metadata={"title": "Test"},
        publish=False,
    )

    assert result["deposition_id"] == 42
    assert result["published"] is False
    assert session.post.call_count == 1
    session.delete.assert_not_called()


def test_upload_release_publishes_only_when_requested(tmp_path):
    release_file = tmp_path / "release.tar.gz"
    release_file.write_bytes(b"release")
    checksum = hashlib.md5(release_file.read_bytes()).hexdigest()
    session = Mock()
    session.headers = {}
    session.post.side_effect = [
        FakeResponse(
            201,
            {
                "id": 43,
                "links": {
                    "bucket": "https://sandbox.zenodo.org/api/files/bucket-id",
                    "html": "https://sandbox.zenodo.org/deposit/43",
                },
            },
        ),
        FakeResponse(
            202,
            {
                "id": 43,
                "doi": "10.5072/zenodo.43",
                "links": {"record_html": "https://sandbox.zenodo.org/records/43"},
            },
        ),
    ]
    session.put.return_value = FakeResponse(
        200,
        {
            "key": release_file.name,
            "size": release_file.stat().st_size,
            "checksum": f"md5:{checksum}",
        },
    )

    result = upload_release(
        make_client(session),
        [release_file],
        metadata={"title": "Test"},
        publish=True,
    )

    assert result["published"] is True
    assert result["doi"] == "10.5072/zenodo.43"
    assert session.post.call_count == 2


def test_missing_bucket_url_cleans_up_new_draft(tmp_path):
    release_file = tmp_path / "release.tar.gz"
    release_file.write_bytes(b"release")
    session = Mock()
    session.headers = {}
    session.post.return_value = FakeResponse(201, {"id": 44, "links": {}})
    session.delete.return_value = FakeResponse(204)

    with pytest.raises(ZenodoError, match="bucket URL"):
        upload_release(
            make_client(session),
            [release_file],
            metadata={"title": "Test"},
            publish=False,
        )

    session.delete.assert_called_once()


def test_publish_existing_draft_does_not_create_or_upload():
    session = Mock()
    session.headers = {}
    session.post.return_value = FakeResponse(
        202,
        {
            "id": 45,
            "doi": "10.5072/zenodo.45",
            "links": {"record_html": "https://sandbox.zenodo.org/records/45"},
        },
    )

    result = publish_existing_draft(make_client(session), 45)

    assert result["deposition_id"] == 45
    assert result["published"] is True
    assert result["doi"] == "10.5072/zenodo.45"
    session.put.assert_not_called()
    session.delete.assert_not_called()


def test_update_existing_draft_replaces_files_metadata_and_removes_obsolete(tmp_path):
    release_file = tmp_path / "rsv-smh-2025-07-27.tar.gz"
    release_file.write_bytes(b"release")
    checksum = hashlib.md5(release_file.read_bytes()).hexdigest()
    session = Mock()
    session.headers = {}
    session.get.side_effect = [
        FakeResponse(
            200,
            {
                "id": 46,
                "submitted": False,
                "links": {
                    "bucket": "https://sandbox.zenodo.org/api/files/bucket-id",
                    "html": "https://sandbox.zenodo.org/uploads/46",
                },
                "files": [],
            },
        ),
        FakeResponse(
            200,
            {
                "id": 46,
                "files": [
                    {
                        "id": "obsolete-id",
                        "filename": "rsv-smh-generated-output.tar.gz",
                    }
                ],
            },
        ),
    ]
    session.put.side_effect = [
        FakeResponse(
            200,
            {
                "id": 46,
                "metadata": {"title": "Updated"},
                "links": {"html": "https://sandbox.zenodo.org/uploads/46"},
            },
        ),
        FakeResponse(
            200,
            {
                "key": release_file.name,
                "size": release_file.stat().st_size,
                "checksum": f"md5:{checksum}",
            },
        ),
    ]
    session.delete.return_value = FakeResponse(204)

    result = update_existing_draft(
        make_client(session),
        46,
        [release_file],
        metadata={"title": "Updated"},
    )

    assert result["published"] is False
    assert result["removed_files"] == ["rsv-smh-generated-output.tar.gz"]
    assert result["metadata"]["title"] == "Updated"
    session.delete.assert_called_once()


def test_update_existing_draft_skips_files_with_matching_checksums(tmp_path):
    release_file = tmp_path / "rsv-smh-2025-07-27.tar.gz"
    release_file.write_bytes(b"release")
    checksum = hashlib.md5(release_file.read_bytes()).hexdigest()
    existing_file = {
        "id": "existing-id",
        "filename": release_file.name,
        "checksum": checksum,
    }
    session = Mock()
    session.headers = {}
    session.get.side_effect = [
        FakeResponse(
            200,
            {
                "id": 47,
                "submitted": False,
                "links": {
                    "bucket": "https://sandbox.zenodo.org/api/files/bucket-id",
                    "html": "https://sandbox.zenodo.org/uploads/47",
                },
                "files": [existing_file],
            },
        ),
        FakeResponse(200, {"id": 47, "files": [existing_file]}),
    ]
    session.put.return_value = FakeResponse(
        200,
        {
            "id": 47,
            "metadata": {"title": "Updated"},
            "links": {"html": "https://sandbox.zenodo.org/uploads/47"},
        },
    )

    result = update_existing_draft(
        make_client(session),
        47,
        [release_file],
        metadata={"title": "Updated"},
    )

    assert result["files"] == []
    assert result["unchanged_files"] == [release_file.name]
    assert session.put.call_count == 1
    session.delete.assert_not_called()


def test_client_refuses_unexpected_upload_host(tmp_path):
    release_file = tmp_path / "release.tar.gz"
    release_file.write_bytes(b"release")
    session = Mock()
    session.headers = {}

    with pytest.raises(ZenodoError, match="unexpected upload host"):
        make_client(session).upload_file(
            "https://example.com/api/files/bucket-id", release_file
        )

    session.put.assert_not_called()
