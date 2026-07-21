"""Tests for prefect_flows_runner.py — the PrefectDbtRunner-based flow file.

Covers:
  * _build_output / build_profile_dict (pure functions, no I/O)
  * dbtjob_v2_runner (block loading, profile.yml writing, SSL cert handling,
    argv splitting, dbt-test failure state)
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from proxy.prefect_flows_runner import (
    DBT_TARGET,
    _build_output,
    build_profile_dict,
    dbtjob_v2_runner,
)


# =============================================================================
# _build_output — postgres
# =============================================================================


def test_build_output_postgres_maps_username_to_user():
    """dbt-postgres field is `user`, airbyte spec sends `username` — must be renamed."""
    creds = {"username": "airbyte_user", "password": "pw", "host": "h", "port": 5432}
    output = _build_output("postgres", "analytics", creds, {}, threads=4)
    assert output["user"] == "airbyte_user"
    assert "username" not in output


def test_build_output_postgres_pops_airbyte_schema_and_uses_dbt_schema():
    """airbyte's `schema` in creds is unrelated to dbt's target schema; must
    be popped and replaced with the caller-supplied schema."""
    creds = {"username": "u", "password": "pw", "schema": "airbyte_schema"}
    output = _build_output("postgres", "dbt_schema", creds, {}, threads=4)
    assert output["schema"] == "dbt_schema"


def test_build_output_postgres_strips_sslrootcert_content():
    """sslrootcert_content is our internal transport for the PEM body — not a
    valid dbt-postgres field. Must not leak into profiles.yml."""
    creds = {
        "username": "u",
        "password": "pw",
        "sslrootcert": "/tmp/cert.pem",
        "sslrootcert_content": "-----BEGIN CERTIFICATE-----\n...",
    }
    output = _build_output("postgres", "analytics", creds, {}, threads=4)
    assert "sslrootcert_content" not in output
    assert output["sslrootcert"] == "/tmp/cert.pem"


def test_build_output_postgres_merges_extras():
    """profile-shaping extras must merge into the output dict."""
    creds = {"username": "u", "password": "pw"}
    extras = {"connect_timeout": 10}
    output = _build_output("postgres", "analytics", creds, extras, threads=4)
    assert output["connect_timeout"] == 10


# =============================================================================
# _build_output — bigquery
# =============================================================================


def test_build_output_bigquery_puts_creds_as_keyfile_json():
    """BQ's `creds` IS the service-account JSON. It's nested under keyfile_json,
    not merged flat like postgres."""
    creds = {"type": "service_account", "project_id": "proj", "private_key": "k"}
    output = _build_output("bigquery", "analytics", creds, {}, threads=4)
    assert output["method"] == "service-account-json"
    assert output["keyfile_json"] == creds
    assert output["schema"] == "analytics"
    assert output["type"] == "bigquery"


def test_build_output_bigquery_injects_extras_location_and_priority():
    creds = {"type": "service_account"}
    extras = {"location": "us-central1", "priority": "batch"}
    output = _build_output("bigquery", "analytics", creds, extras, threads=4)
    assert output["location"] == "us-central1"
    assert output["priority"] == "batch"


# =============================================================================
# _build_output — unsupported
# =============================================================================


def test_build_output_unsupported_wtype_raises():
    with pytest.raises(ValueError, match="Unsupported warehouse type: snowflake"):
        _build_output("snowflake", "analytics", {}, {}, threads=4)


# =============================================================================
# build_profile_dict
# =============================================================================


def test_build_profile_dict_shape():
    """profiles.yml top level = profile_name; nested `target` label matches the
    single output key so dbt can resolve it."""
    creds = {"username": "u", "password": "pw"}
    profile = build_profile_dict(
        profile_name="dalgo",
        wtype="postgres",
        target=DBT_TARGET,
        schema="analytics",
        creds=creds,
        extras={},
    )
    assert list(profile.keys()) == ["dalgo"]
    assert profile["dalgo"]["target"] == DBT_TARGET
    assert list(profile["dalgo"]["outputs"].keys()) == [DBT_TARGET]
    assert profile["dalgo"]["outputs"][DBT_TARGET]["schema"] == "analytics"


# =============================================================================
# dbtjob_v2_runner
# =============================================================================


def _make_task_config(tmp_path, commands=None, slug="dbt-run"):
    """Helper: write a minimal dbt_project.yml in tmp_path and return a
    task_config pointing at it."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text(yaml.safe_dump({"profile": "dalgo"}))
    profiles_dir = tmp_path / "profiles"
    return {
        "slug": slug,
        "commands": commands or ["/venv/bin/dbt run --full-refresh"],
        "env": {"dbt-profile-secret-block": "dbt-profile-test"},
        "project_dir": str(project_dir),
        "profiles_dir": str(profiles_dir),
    }


def _make_block_value(wtype="postgres", schema="analytics", creds=None, extras=None):
    return {
        "wtype": wtype,
        "default_schema": schema,
        "creds": creds if creds is not None else {"username": "u", "password": "pw"},
        "extras": extras if extras is not None else {},
    }


@patch("proxy.prefect_flows_runner.PrefectDbtRunner")
@patch("proxy.prefect_flows_runner.Secret")
def test_dbtjob_v2_runner_block_value_as_dict(mock_secret, mock_runner_cls, tmp_path):
    """Prefect stores block values in a JSON column — .get() may return a dict
    (deserialized) or a JSON string. The dict path must work."""
    block_value = _make_block_value()
    mock_secret.load.return_value.get.return_value = block_value  # dict
    mock_runner = MagicMock()
    mock_runner_cls.return_value = mock_runner

    task_config = _make_task_config(tmp_path)
    dbtjob_v2_runner.fn(task_config, task_config["slug"])

    mock_secret.load.assert_called_once_with("dbt-profile-test")
    # binary token stripped, remaining shell tokens passed as argv
    mock_runner.invoke.assert_called_once_with(["run", "--full-refresh"])

    # profiles.yml was written with the correct schema
    written = yaml.safe_load((tmp_path / "profiles" / "profiles.yml").read_text())
    assert written["dalgo"]["outputs"][DBT_TARGET]["schema"] == "analytics"
    assert written["dalgo"]["outputs"][DBT_TARGET]["type"] == "postgres"


@patch("proxy.prefect_flows_runner.PrefectDbtRunner")
@patch("proxy.prefect_flows_runner.Secret")
def test_dbtjob_v2_runner_block_value_as_json_string(
    mock_secret, mock_runner_cls, tmp_path
):
    """Same runner path must also work when .get() returns a JSON string."""
    block_value = _make_block_value(wtype="bigquery", schema="warehouse")
    mock_secret.load.return_value.get.return_value = json.dumps(block_value)  # string
    mock_runner_cls.return_value = MagicMock()

    task_config = _make_task_config(tmp_path)
    dbtjob_v2_runner.fn(task_config, task_config["slug"])

    written = yaml.safe_load((tmp_path / "profiles" / "profiles.yml").read_text())
    assert written["dalgo"]["outputs"][DBT_TARGET]["type"] == "bigquery"
    assert written["dalgo"]["outputs"][DBT_TARGET]["schema"] == "warehouse"


@patch("proxy.prefect_flows_runner.PrefectDbtRunner")
@patch("proxy.prefect_flows_runner.Secret")
def test_dbtjob_v2_runner_writes_ssl_cert_and_rewrites_path(
    mock_secret, mock_runner_cls, tmp_path
):
    """postgres SSL: cert content lives inside creds.sslrootcert_content;
    runner writes it to creds.sslrootcert path, then rewrites the profile
    output's `sslrootcert` field to that path."""
    cert_path = str(tmp_path / "certs" / "sslrootcert.pem")
    cert_content = "-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----"
    creds = {
        "username": "u",
        "password": "pw",
        "sslrootcert": cert_path,
        "sslrootcert_content": cert_content,
    }
    block_value = _make_block_value(wtype="postgres", creds=creds)
    mock_secret.load.return_value.get.return_value = block_value
    mock_runner_cls.return_value = MagicMock()

    task_config = _make_task_config(tmp_path)
    dbtjob_v2_runner.fn(task_config, task_config["slug"])

    # cert written to disk at the backend-computed path
    assert Path(cert_path).read_text() == cert_content

    # profiles.yml points at the cert path (not the content)
    written = yaml.safe_load((tmp_path / "profiles" / "profiles.yml").read_text())
    output = written["dalgo"]["outputs"][DBT_TARGET]
    assert output["sslrootcert"] == cert_path
    assert "sslrootcert_content" not in output


@patch("proxy.prefect_flows_runner.PrefectDbtRunner")
@patch("proxy.prefect_flows_runner.Secret")
def test_dbtjob_v2_runner_dbt_test_failure_returns_completed_state(
    mock_secret, mock_runner_cls, tmp_path
):
    """dbt-test failures don't fail the flow — they return a COMPLETED state
    labelled DBT_TEST_FAILED so downstream tasks continue."""
    mock_secret.load.return_value.get.return_value = _make_block_value()
    mock_runner = MagicMock()
    mock_runner.invoke.side_effect = RuntimeError("test failed")
    mock_runner_cls.return_value = mock_runner

    task_config = _make_task_config(tmp_path, slug="dbt-test")
    result = dbtjob_v2_runner.fn(task_config, "dbt-test")

    assert result.name == "DBT_TEST_FAILED"
    assert "dbt test failed" in result.message.lower()


@patch("proxy.prefect_flows_runner.PrefectDbtRunner")
@patch("proxy.prefect_flows_runner.Secret")
def test_dbtjob_v2_runner_non_test_failure_reraises(
    mock_secret, mock_runner_cls, tmp_path
):
    """Non-test failures must propagate — silently swallowing them would hide
    real dbt run/seed/snapshot errors."""
    mock_secret.load.return_value.get.return_value = _make_block_value()
    mock_runner = MagicMock()
    mock_runner.invoke.side_effect = RuntimeError("run failed")
    mock_runner_cls.return_value = mock_runner

    task_config = _make_task_config(tmp_path, slug="dbt-run")
    with pytest.raises(RuntimeError, match="run failed"):
        dbtjob_v2_runner.fn(task_config, "dbt-run")
