"""Tests for prefect_flows.py - SSL cert handling in dbtjob_v1"""

import os
from unittest.mock import patch, MagicMock

from proxy.prefect_flows import dbtjob_v1


def _make_task_config(**overrides):
    config = {
        "slug": "dbt-run",
        "cli_profile_block": "test-block",
        "commands": ["dbt run"],
        "env": {},
        "working_dir": "/tmp/working",
        "profiles_dir": "/tmp/profiles",
        "project_dir": "/tmp/project",
    }
    config.update(overrides)
    return config


@patch("proxy.prefect_flows.DbtCoreOperation")
@patch("proxy.prefect_flows.DbtCliProfile")
def test_dbtjob_v1_writes_ssl_cert_to_disk(mock_dbt_cli_profile, mock_dbt_core_op, tmp_path):
    """When sslrootcert_content is in extras, it should be written to disk
    and removed from extras before running dbt"""
    cert_content = "-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----"
    cert_path = str(tmp_path / "sslrootcert.pem")

    mock_block = MagicMock()
    mock_block.target_configs.extras = {
        "host": "localhost",
        "sslmode": "verify-ca",
        "sslrootcert": cert_path,
        "sslrootcert_content": cert_content,
    }
    mock_dbt_cli_profile.load.return_value = mock_block

    mock_op = MagicMock()
    mock_op.profiles_dir.__truediv__ = lambda self, x: tmp_path / x
    mock_op.run.return_value = "success"
    mock_dbt_core_op.return_value = mock_op

    task_config = _make_task_config()
    # Call the underlying function directly (not as a Prefect task)
    result = dbtjob_v1.fn(task_config, "dbt-run")

    # cert should be written to disk
    assert os.path.exists(cert_path)
    with open(cert_path) as f:
        assert f.read() == cert_content

    # sslrootcert_content should be popped from extras (not leaked to profiles.yml)
    assert "sslrootcert_content" not in mock_block.target_configs.extras

    # sslrootcert path should remain
    assert mock_block.target_configs.extras["sslrootcert"] == cert_path

    assert result == "success"


@patch("proxy.prefect_flows.DbtCoreOperation")
@patch("proxy.prefect_flows.DbtCliProfile")
def test_dbtjob_v1_no_ssl_cert(mock_dbt_cli_profile, mock_dbt_core_op, tmp_path):
    """When there's no sslrootcert_content in extras, no cert file should be written"""
    mock_block = MagicMock()
    mock_block.target_configs.extras = {
        "host": "localhost",
        "user": "test",
    }
    mock_dbt_cli_profile.load.return_value = mock_block

    mock_op = MagicMock()
    mock_op.profiles_dir.__truediv__ = lambda self, x: tmp_path / x
    mock_op.run.return_value = "success"
    mock_dbt_core_op.return_value = mock_op

    task_config = _make_task_config()
    result = dbtjob_v1.fn(task_config, "dbt-run")

    assert result == "success"
    # no cert written
    assert not os.path.exists(str(tmp_path / "sslrootcert.pem"))


@patch("proxy.prefect_flows.DbtCoreOperation")
@patch("proxy.prefect_flows.DbtCliProfile")
def test_dbtjob_v1_no_extras(mock_dbt_cli_profile, mock_dbt_core_op, tmp_path):
    """When target_configs has no extras, should still work"""
    mock_block = MagicMock()
    mock_block.target_configs.extras = None
    mock_dbt_cli_profile.load.return_value = mock_block

    mock_op = MagicMock()
    mock_op.profiles_dir.__truediv__ = lambda self, x: tmp_path / x
    mock_op.run.return_value = "success"
    mock_dbt_core_op.return_value = mock_op

    task_config = _make_task_config()
    result = dbtjob_v1.fn(task_config, "dbt-run")

    assert result == "success"


@patch("proxy.prefect_flows.DbtCoreOperation")
@patch("proxy.prefect_flows.DbtCliProfile")
def test_dbtjob_v1_ssl_cert_fallback_to_org_project_dir(mock_dbt_cli_profile, mock_dbt_core_op, tmp_path):
    """When sslrootcert_content exists but sslrootcert path is missing,
    should fall back to {project_dir}/../sslrootcert.pem"""
    cert_content = "-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----"
    project_dir = str(tmp_path / "org" / "dbtrepo")
    os.makedirs(project_dir, exist_ok=True)

    mock_block = MagicMock()
    mock_block.target_configs.extras = {
        "host": "localhost",
        "sslmode": "verify-ca",
        "sslrootcert_content": cert_content,
    }
    mock_dbt_cli_profile.load.return_value = mock_block

    mock_op = MagicMock()
    mock_op.profiles_dir.__truediv__ = lambda self, x: tmp_path / x
    mock_op.run.return_value = "success"
    mock_dbt_core_op.return_value = mock_op

    task_config = _make_task_config(project_dir=project_dir)
    result = dbtjob_v1.fn(task_config, "dbt-run")

    # cert should be written to parent of project_dir
    expected_path = os.path.join(project_dir, "..", "sslrootcert.pem")
    assert os.path.exists(expected_path)
    with open(expected_path) as f:
        assert f.read() == cert_content

    # extras should have sslrootcert set to the fallback path
    assert mock_block.target_configs.extras["sslrootcert"] == expected_path
    assert "sslrootcert_content" not in mock_block.target_configs.extras

    assert result == "success"
