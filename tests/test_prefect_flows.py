"""Tests for prefect_flows.py"""

import os
import pytest
from unittest.mock import patch, MagicMock

from proxy.prefect_flows import (
    dbtjob_v1,
    _is_airbyte_sync_task,
    _run_tasks_sequentially,
    _run_tasks_with_sync_tolerance,
    deployment_schedule_flow_v4,
    AIRBYTECONNECTION,
    DBTCORE,
    SHELLOPERATION,
)


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
def test_dbtjob_v1_ssl_cert_fallback_to_org_project_dir(
    mock_dbt_cli_profile, mock_dbt_core_op, tmp_path
):
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


# =============================================================================
# Tests for deployment_schedule_flow_v4 orchestration logic
# =============================================================================


def _make_sync_task(connection_id, seq=1):
    return {
        "type": AIRBYTECONNECTION,
        "slug": "airbyte-sync",
        "connection_id": connection_id,
        "seq": seq,
        "airbyte_server_block": "test-server",
        "timeout": 15,
    }


def _make_dbt_task(slug="dbt-run", seq=2):
    return {
        "type": DBTCORE,
        "slug": slug,
        "seq": seq,
        "cli_profile_block": "test-block",
        "commands": ["dbt run"],
        "env": {},
        "working_dir": "/tmp/working",
        "profiles_dir": "/tmp/profiles",
        "project_dir": "/tmp/project",
    }


# --- _is_airbyte_sync_task ---


def test_is_airbyte_sync_task_true():
    assert _is_airbyte_sync_task({"type": AIRBYTECONNECTION, "slug": "airbyte-sync"}) is True


def test_is_airbyte_sync_task_false_for_clear():
    assert _is_airbyte_sync_task({"type": AIRBYTECONNECTION, "slug": "airbyte-clear"}) is False


def test_is_airbyte_sync_task_false_for_dbt():
    assert _is_airbyte_sync_task({"type": DBTCORE, "slug": "dbt-run"}) is False


# --- _run_tasks_sequentially ---


@patch("proxy.prefect_flows._run_task")
@patch("proxy.prefect_flows.sleep")
def test_run_tasks_sequentially_all_succeed(mock_sleep, mock_run_task):
    tasks = [_make_sync_task("conn-1"), _make_dbt_task()]
    _run_tasks_sequentially(tasks)

    assert mock_run_task.call_count == 2
    assert mock_sleep.call_count == 2


@patch("proxy.prefect_flows._run_task")
@patch("proxy.prefect_flows.sleep")
def test_run_tasks_sequentially_fails_fast(mock_sleep, mock_run_task):
    """First task fails — second task should not run"""
    mock_run_task.side_effect = RuntimeError("sync failed")

    tasks = [_make_sync_task("conn-1"), _make_dbt_task()]
    with pytest.raises(RuntimeError, match="sync failed"):
        _run_tasks_sequentially(tasks)

    assert mock_run_task.call_count == 1


# --- _run_tasks_with_sync_tolerance ---


@patch("proxy.prefect_flows._run_task")
@patch("proxy.prefect_flows.sleep")
def test_sync_tolerance_all_syncs_succeed(mock_sleep, mock_run_task):
    """All syncs pass — transforms should run"""
    tasks = [
        _make_sync_task("conn-1", seq=1),
        _make_sync_task("conn-2", seq=1),
        _make_dbt_task(seq=2),
    ]
    _run_tasks_with_sync_tolerance(tasks)

    assert mock_run_task.call_count == 3


@patch("proxy.prefect_flows._run_task")
@patch("proxy.prefect_flows.sleep")
def test_sync_tolerance_one_sync_fails_continues_rest(mock_sleep, mock_run_task):
    """First sync fails — second sync should still run, then raise before transforms"""

    def side_effect(task_config):
        if task_config.get("connection_id") == "conn-1":
            raise RuntimeError("conn-1 failed")

    mock_run_task.side_effect = side_effect

    tasks = [
        _make_sync_task("conn-1", seq=1),
        _make_sync_task("conn-2", seq=1),
        _make_dbt_task(seq=2),
    ]

    with pytest.raises(RuntimeError, match="1 airbyte sync\\(s\\) failed"):
        _run_tasks_with_sync_tolerance(tasks)

    # both syncs attempted, dbt NOT attempted
    sync_calls = [c for c in mock_run_task.call_args_list if c[0][0].get("connection_id")]
    assert len(sync_calls) == 2

    dbt_calls = [c for c in mock_run_task.call_args_list if c[0][0]["type"] == DBTCORE]
    assert len(dbt_calls) == 0


@patch("proxy.prefect_flows._run_task")
@patch("proxy.prefect_flows.sleep")
def test_sync_tolerance_all_syncs_fail(mock_sleep, mock_run_task):
    """All syncs fail — error message should mention count"""
    mock_run_task.side_effect = RuntimeError("failed")

    tasks = [
        _make_sync_task("conn-1", seq=1),
        _make_sync_task("conn-2", seq=1),
        _make_dbt_task(seq=2),
    ]

    with pytest.raises(RuntimeError, match="2 airbyte sync\\(s\\) failed"):
        _run_tasks_with_sync_tolerance(tasks)


@patch("proxy.prefect_flows._run_task")
@patch("proxy.prefect_flows.sleep")
def test_sync_tolerance_transform_fails(mock_sleep, mock_run_task):
    """Syncs pass but transform fails — should raise the transform error"""

    def side_effect(task_config):
        if task_config["type"] == DBTCORE:
            raise RuntimeError("dbt failed")

    mock_run_task.side_effect = side_effect

    tasks = [
        _make_sync_task("conn-1", seq=1),
        _make_dbt_task(seq=2),
    ]

    with pytest.raises(RuntimeError, match="dbt failed"):
        _run_tasks_with_sync_tolerance(tasks)


@patch("proxy.prefect_flows._run_task")
@patch("proxy.prefect_flows.sleep")
def test_sync_tolerance_no_sync_tasks(mock_sleep, mock_run_task):
    """No sync tasks — should just run transforms sequentially"""
    tasks = [_make_dbt_task(seq=1)]
    _run_tasks_with_sync_tolerance(tasks)

    assert mock_run_task.call_count == 1


@patch("proxy.prefect_flows._run_task")
@patch("proxy.prefect_flows.sleep")
def test_sync_tolerance_no_transform_tasks(mock_sleep, mock_run_task):
    """Only sync tasks, no transforms — should work fine"""
    tasks = [_make_sync_task("conn-1", seq=1), _make_sync_task("conn-2", seq=1)]
    _run_tasks_with_sync_tolerance(tasks)

    assert mock_run_task.call_count == 2


# --- deployment_schedule_flow_v4 flag routing ---


@patch("proxy.prefect_flows._run_tasks_with_sync_tolerance")
@patch("proxy.prefect_flows._run_tasks_sequentially")
def test_v4_uses_sequential_by_default(mock_sequential, mock_tolerant):
    config = {"tasks": [_make_sync_task("conn-1")]}
    deployment_schedule_flow_v4.fn(config)

    mock_sequential.assert_called_once()
    mock_tolerant.assert_not_called()


@patch("proxy.prefect_flows._run_tasks_with_sync_tolerance")
@patch("proxy.prefect_flows._run_tasks_sequentially")
def test_v4_uses_sync_tolerance_when_flag_set(mock_sequential, mock_tolerant):
    config = {"tasks": [_make_sync_task("conn-1")], "continue_on_sync_failure": True}
    deployment_schedule_flow_v4.fn(config)

    mock_tolerant.assert_called_once()
    mock_sequential.assert_not_called()


@patch("proxy.prefect_flows._run_tasks_with_sync_tolerance")
@patch("proxy.prefect_flows._run_tasks_sequentially")
def test_v4_uses_sequential_when_flag_false(mock_sequential, mock_tolerant):
    config = {"tasks": [_make_sync_task("conn-1")], "continue_on_sync_failure": False}
    deployment_schedule_flow_v4.fn(config)

    mock_sequential.assert_called_once()
    mock_tolerant.assert_not_called()
