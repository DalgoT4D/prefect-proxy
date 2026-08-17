"""Tests for prefect_flows_runner.py — the PrefectDbtRunner-based flow file.

Covers:
  * _build_output / build_profile_dict (pure functions, no I/O)
  * dbtjob_v2_runner (block loading, profile.yml writing, SSL cert handling,
    argv splitting, dbt-test failure state)
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from proxy.prefect_flows_runner import (
    AIRBYTECONNECTION,
    DBTCORE,
    DBT_TARGET,
    SHELLOPERATION,
    _build_output,
    _run_task_runner,
    _run_tasks_sequentially,
    _run_tasks_with_sync_tolerance,
    build_profile_dict,
    dbtjob_v2_runner,
    deployment_schedule_flow_v5,
    run_airbyte_connection_flow_v1,
    shellopjob,
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


@patch("proxy.prefect_flows_runner.ShellOperation")
@patch("proxy.prefect_flows_runner.Secret")
def test_dbtjob_v2_runner_block_value_as_dict(mock_secret, mock_shell_cls, tmp_path):
    """Prefect stores block values in a JSON column — .get() may return a dict
    (deserialized) or a JSON string. The dict path must work."""
    block_value = _make_block_value()
    mock_secret.load.return_value.get.return_value = block_value  # dict

    task_config = _make_task_config(tmp_path)
    dbtjob_v2_runner.fn(task_config, task_config["slug"])

    mock_secret.load.assert_called_once_with("dbt-profile-test")
    # binary token from task_config stripped; ShellOperation called once with
    # a command containing the remaining tokens
    mock_shell_cls.assert_called_once()
    cmd = mock_shell_cls.call_args.kwargs["commands"][0]
    assert "run" in cmd and "--full-refresh" in cmd
    assert "/venv/bin/dbt" not in cmd  # original binary replaced

    # profiles.yml was written with the correct schema
    written = yaml.safe_load((tmp_path / "profiles" / "profiles.yml").read_text())
    assert written["dalgo"]["outputs"][DBT_TARGET]["schema"] == "analytics"
    assert written["dalgo"]["outputs"][DBT_TARGET]["type"] == "postgres"


@patch("proxy.prefect_flows_runner.ShellOperation")
@patch("proxy.prefect_flows_runner.Secret")
def test_dbtjob_v2_runner_block_value_as_json_string(mock_secret, mock_shell_cls, tmp_path):
    """Same runner path must also work when .get() returns a JSON string."""
    block_value = _make_block_value(wtype="bigquery", schema="warehouse")
    mock_secret.load.return_value.get.return_value = json.dumps(block_value)  # string

    task_config = _make_task_config(tmp_path)
    dbtjob_v2_runner.fn(task_config, task_config["slug"])

    written = yaml.safe_load((tmp_path / "profiles" / "profiles.yml").read_text())
    assert written["dalgo"]["outputs"][DBT_TARGET]["type"] == "bigquery"
    assert written["dalgo"]["outputs"][DBT_TARGET]["schema"] == "warehouse"


@patch("proxy.prefect_flows_runner.ShellOperation")
@patch("proxy.prefect_flows_runner.Secret")
def test_dbtjob_v2_runner_writes_ssl_cert_and_rewrites_path(mock_secret, mock_shell_cls, tmp_path):
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

    task_config = _make_task_config(tmp_path)
    dbtjob_v2_runner.fn(task_config, task_config["slug"])

    # cert written to disk at the backend-computed path
    assert Path(cert_path).read_text() == cert_content

    # profiles.yml points at the cert path (not the content)
    written = yaml.safe_load((tmp_path / "profiles" / "profiles.yml").read_text())
    output = written["dalgo"]["outputs"][DBT_TARGET]
    assert output["sslrootcert"] == cert_path
    assert "sslrootcert_content" not in output


@patch("proxy.prefect_flows_runner.ShellOperation")
@patch("proxy.prefect_flows_runner.Secret")
def test_dbtjob_v2_runner_dbt_test_failure_returns_completed_state(
    mock_secret, mock_shell_cls, tmp_path
):
    """dbt-test failures don't fail the flow — they return a COMPLETED state
    labelled DBT_TEST_FAILED so downstream tasks continue."""
    mock_secret.load.return_value.get.return_value = _make_block_value()
    mock_shell_cls.return_value.run.side_effect = RuntimeError("test failed")

    task_config = _make_task_config(tmp_path, slug="dbt-test")
    result = dbtjob_v2_runner.fn(task_config, "dbt-test")

    assert result.name == "DBT_TEST_FAILED"
    assert "dbt test failed" in result.message.lower()


@patch("proxy.prefect_flows_runner.ShellOperation")
@patch("proxy.prefect_flows_runner.Secret")
def test_dbtjob_v2_runner_non_test_failure_reraises(mock_secret, mock_shell_cls, tmp_path):
    """Non-test failures must propagate — silently swallowing them would hide
    real dbt run/seed/snapshot errors."""
    mock_secret.load.return_value.get.return_value = _make_block_value()
    mock_shell_cls.return_value.run.side_effect = RuntimeError("run failed")

    task_config = _make_task_config(tmp_path, slug="dbt-run")
    with pytest.raises(RuntimeError, match="run failed"):
        dbtjob_v2_runner.fn(task_config, "dbt-run")


# =============================================================================
# run_airbyte_connection_flow_v1 — block-load + fallback path
# =============================================================================


@pytest.fixture
def _mock_flow_deps():
    """Bundles the four mocks every run_airbyte_connection_flow_v1 test needs.

    Yields a dict so tests can adjust individual mocks (e.g. make aload raise
    ValueError to trigger the fallback branch)."""
    with patch("proxy.prefect_flows_runner.get_run_logger", return_value=MagicMock()), patch(
        "proxy.prefect_flows_runner.AirbyteConnection"
    ) as ab_conn_cls, patch("proxy.prefect_flows_runner.AirbyteServer") as ab_server_cls, patch(
        "proxy.prefect_flows_runner.run_connection_sync"
    ) as run_sync, patch(
        "proxy.prefect_flows_runner._run_post_sync_ops", new_callable=AsyncMock
    ) as post_sync:
        # run_connection_sync.with_options(...)(block) → awaitable result
        run_sync.with_options.return_value = AsyncMock(return_value={"status": "ok"})
        yield {
            "ab_conn_cls": ab_conn_cls,
            "ab_server_cls": ab_server_cls,
            "run_sync": run_sync,
            "post_sync": post_sync,
        }


@pytest.mark.asyncio
async def test_flow_uses_block_extra_when_block_exists(_mock_flow_deps):
    """When the AirbyteConnection block exists, its `.extra` drives post-sync ops —
    the fallback inline construction must NOT be reached."""
    extra = {"env": {"dbt-profile-secret-block": "sec-blk"}, "post_sync_ops": [{"type": "cast"}]}
    loaded_block = MagicMock(extra=extra)
    _mock_flow_deps["ab_conn_cls"].aload = AsyncMock(return_value=loaded_block)

    payload = {"connection_id": "conn-1", "airbyte_server_block": "srv-blk", "timeout": 30}
    result = await run_airbyte_connection_flow_v1.fn(payload)

    assert result == {"status": "ok"}
    _mock_flow_deps["ab_conn_cls"].aload.assert_awaited_once_with("conn-1")
    # Fallback path must NOT run when the block loaded successfully
    _mock_flow_deps["ab_server_cls"].aload.assert_not_called()
    # post-sync ops called with the block's extra content
    _mock_flow_deps["post_sync"].assert_awaited_once_with(
        env=extra["env"], ops=extra["post_sync_ops"]
    )


@pytest.mark.asyncio
async def test_flow_falls_back_to_inline_when_no_block(_mock_flow_deps):
    """Backwards compat: legacy connections have no persisted block. The flow
    must fall back to inline AirbyteConnection construction and skip post-sync
    ops (extra={} → no ops to run)."""
    _mock_flow_deps["ab_conn_cls"].aload = AsyncMock(side_effect=ValueError("no block"))
    _mock_flow_deps["ab_server_cls"].aload = AsyncMock(return_value=MagicMock())
    # Inline-constructed block has default `extra = {}`
    inline_block = MagicMock(extra={})
    _mock_flow_deps["ab_conn_cls"].return_value = inline_block

    payload = {"connection_id": "legacy-conn", "airbyte_server_block": "srv-blk", "timeout": 15}
    result = await run_airbyte_connection_flow_v1.fn(payload)

    assert result == {"status": "ok"}
    _mock_flow_deps["ab_server_cls"].aload.assert_awaited_once_with("srv-blk")
    # post-sync called with empty env+ops (the .get() defaults) — a no-op inside
    _mock_flow_deps["post_sync"].assert_awaited_once_with(env={}, ops=[])


@pytest.mark.asyncio
async def test_flow_reraises_on_sync_failure_and_skips_post_sync(_mock_flow_deps):
    """A sync failure must propagate so Prefect marks the flow-run failed.
    Post-sync ops must NOT run — casting against unsynced data would be a data-integrity bug."""
    _mock_flow_deps["ab_conn_cls"].aload = AsyncMock(return_value=MagicMock(extra={}))
    _mock_flow_deps["run_sync"].with_options.return_value = AsyncMock(
        side_effect=RuntimeError("airbyte sync failed")
    )

    payload = {"connection_id": "conn-1", "airbyte_server_block": "srv-blk", "timeout": 30}
    with pytest.raises(RuntimeError, match="airbyte sync failed"):
        await run_airbyte_connection_flow_v1.fn(payload)

    _mock_flow_deps["post_sync"].assert_not_awaited()


@pytest.mark.asyncio
async def test_flow_swallows_post_sync_errors_and_returns_sync_result(_mock_flow_deps):
    """If post-sync ops fail after a successful sync, the sync result must still
    be returned — the data has landed. Failing the flow here would push users to
    re-run a sync unnecessarily (and potentially double-charge Airbyte credits)."""
    extra = {"env": {"dbt-profile-secret-block": "sec-blk"}, "post_sync_ops": [{"type": "cast"}]}
    _mock_flow_deps["ab_conn_cls"].aload = AsyncMock(return_value=MagicMock(extra=extra))
    _mock_flow_deps["post_sync"].side_effect = RuntimeError("cast SQL failed")

    payload = {"connection_id": "conn-1", "airbyte_server_block": "srv-blk", "timeout": 30}
    result = await run_airbyte_connection_flow_v1.fn(payload)

    assert result == {"status": "ok"}
    _mock_flow_deps["post_sync"].assert_awaited_once()


# =============================================================================
# _run_task_runner — dispatch table for the deployment task-type / slug matrix
# =============================================================================


@pytest.fixture
def _mock_dispatch():
    """Patches all four sinks _run_task_runner can dispatch to. Yields the
    mocks so each test can assert exactly which sink was hit."""
    with patch("proxy.prefect_flows_runner.dbtjob_v2_runner") as dbt, patch(
        "proxy.prefect_flows_runner.shellopjob"
    ) as shell, patch(
        "proxy.prefect_flows_runner.run_airbyte_connection_flow_v1"
    ) as ab_sync, patch(
        "proxy.prefect_flows_runner.run_airbyte_conn_clear"
    ) as ab_clear, patch(
        "proxy.prefect_flows_runner.run_refresh_schema_flow"
    ) as refresh, patch(
        "proxy.prefect_flows_runner.asyncio.run"
    ) as asyncio_run:
        # Route coroutines synchronously so we can inspect what asyncio.run was passed
        asyncio_run.side_effect = lambda coro: coro
        yield {
            "dbt": dbt,
            "shell": shell,
            "ab_sync": ab_sync,
            "ab_clear": ab_clear,
            "refresh": refresh,
            "asyncio_run": asyncio_run,
        }


def test_run_task_runner_dispatches_dbtcore(_mock_dispatch):
    """DBTCORE tasks must route to dbtjob_v2_runner. Wrong dispatch → dbt tasks silently skip."""
    task = {"type": DBTCORE, "slug": "dbt-run"}
    _run_task_runner(task)
    _mock_dispatch["dbt"].assert_called_once_with(task, "dbt-run")


def test_run_task_runner_dispatches_shell(_mock_dispatch):
    """SHELLOPERATION tasks must route to shellopjob. Wrong dispatch → git-pull silently skips
    and dbt runs against stale code."""
    task = {"type": SHELLOPERATION, "slug": "git-pull"}
    _run_task_runner(task)
    _mock_dispatch["shell"].assert_called_once_with(task, "git-pull")


def test_run_task_runner_dispatches_airbyte_sync(_mock_dispatch):
    """The Airbyte sync path — the load-path this whole feature protects."""
    task = {"type": AIRBYTECONNECTION, "slug": "airbyte-sync"}
    _run_task_runner(task)
    _mock_dispatch["ab_sync"].assert_called_once_with(task)
    _mock_dispatch["asyncio_run"].assert_called_once()


def test_run_task_runner_dispatches_airbyte_clear(_mock_dispatch):
    task = {"type": AIRBYTECONNECTION, "slug": "airbyte-clear"}
    _run_task_runner(task)
    _mock_dispatch["ab_clear"].assert_called_once_with(task)


def test_run_task_runner_dispatches_update_schema(_mock_dispatch):
    task = {"type": AIRBYTECONNECTION, "slug": "update-schema", "catalog_diff": {"foo": "bar"}}
    _run_task_runner(task)
    _mock_dispatch["refresh"].assert_called_once_with(task, catalog_diff={"foo": "bar"})


def test_run_task_runner_raises_on_unknown_airbyte_slug(_mock_dispatch):
    """Fail loud when an unrecognized AIRBYTECONNECTION slug appears — silently
    skipping would hide real bugs in the backend's task-config builder."""
    task = {"type": AIRBYTECONNECTION, "slug": "made-up-slug"}
    with pytest.raises(ValueError, match="Unsupported AIRBYTECONNECTION slug"):
        _run_task_runner(task)


def test_run_task_runner_raises_on_unknown_type(_mock_dispatch):
    """Fail loud on unknown top-level type."""
    task = {"type": "invented-type", "slug": "x"}
    with pytest.raises(ValueError, match="Unknown task type"):
        _run_task_runner(task)


# =============================================================================
# _run_tasks_sequentially — fail-fast semantics
# =============================================================================


@patch("proxy.prefect_flows_runner.sleep")
@patch("proxy.prefect_flows_runner._run_task_runner")
def test_run_tasks_sequentially_stops_on_first_error(mock_runner, _mock_sleep):
    """First task raises → second task must NOT be attempted. If broken, downstream
    tasks run against stale/failed prereqs (e.g. dbt-run after failed git-pull)."""
    tasks = [{"type": DBTCORE, "slug": "dbt-deps"}, {"type": DBTCORE, "slug": "dbt-run"}]
    mock_runner.side_effect = [RuntimeError("first task failed"), None]

    with pytest.raises(RuntimeError, match="first task failed"):
        _run_tasks_sequentially(tasks)

    # Only the first task should have been attempted
    assert mock_runner.call_count == 1


@patch("proxy.prefect_flows_runner.sleep")
@patch("proxy.prefect_flows_runner._run_task_runner")
def test_run_tasks_sequentially_happy_path(mock_runner, _mock_sleep):
    """All tasks called in order when none fail."""
    t1 = {"type": DBTCORE, "slug": "dbt-deps"}
    t2 = {"type": DBTCORE, "slug": "dbt-run"}
    t3 = {"type": DBTCORE, "slug": "dbt-test"}

    _run_tasks_sequentially([t1, t2, t3])

    assert mock_runner.call_count == 3
    assert [c.args[0] for c in mock_runner.call_args_list] == [t1, t2, t3]


# =============================================================================
# _run_tasks_with_sync_tolerance — best-effort sync then transform
# =============================================================================


@patch("proxy.prefect_flows_runner.sleep")
@patch("proxy.prefect_flows_runner._run_task_runner")
def test_run_tasks_with_sync_tolerance_collects_sync_errors_then_skips_transforms(
    mock_runner, _mock_sleep
):
    """If ANY sync fails, transforms must NOT run — casting/dbt against failed
    sync data corrupts the warehouse. Both syncs are still attempted so all
    errors surface in one flow run."""
    sync1 = {"type": AIRBYTECONNECTION, "slug": "airbyte-sync", "connection_id": "c1"}
    sync2 = {"type": AIRBYTECONNECTION, "slug": "airbyte-sync", "connection_id": "c2"}
    dbt_run = {"type": DBTCORE, "slug": "dbt-run"}

    mock_runner.side_effect = [
        RuntimeError("sync 1 failed"),
        RuntimeError("sync 2 failed"),
        # dbt-run mustn't be reached
    ]

    with pytest.raises(RuntimeError, match="2 airbyte sync\\(s\\) failed"):
        _run_tasks_with_sync_tolerance([sync1, sync2, dbt_run])

    # Both syncs attempted; transform NOT reached
    assert mock_runner.call_count == 2


@patch("proxy.prefect_flows_runner.sleep")
@patch("proxy.prefect_flows_runner._run_task_runner")
def test_run_tasks_with_sync_tolerance_happy_path_runs_transforms(mock_runner, _mock_sleep):
    """All syncs succeed → transforms run after."""
    sync1 = {"type": AIRBYTECONNECTION, "slug": "airbyte-sync", "connection_id": "c1"}
    dbt_run = {"type": DBTCORE, "slug": "dbt-run"}
    dbt_test = {"type": DBTCORE, "slug": "dbt-test"}

    _run_tasks_with_sync_tolerance([sync1, dbt_run, dbt_test])

    assert mock_runner.call_count == 3
    # syncs first, then transforms — even if input order was mixed
    slugs = [c.args[0]["slug"] for c in mock_runner.call_args_list]
    assert slugs.index("airbyte-sync") < slugs.index("dbt-run")


# =============================================================================
# deployment_schedule_flow_v5 — the actual Prefect entrypoint
# =============================================================================


@patch("proxy.prefect_flows_runner._run_tasks_sequentially")
@patch("proxy.prefect_flows_runner._run_tasks_with_sync_tolerance")
def test_deployment_schedule_flow_sorts_tasks_by_seq(mock_tolerant, mock_sequential):
    """Tasks with out-of-order seq must be sorted before dispatch. This ordering
    is critical: git-pull (seq=1) MUST run before dbt-run (seq=4). If sort
    breaks, dbt runs against stale code."""
    tasks_out_of_order = [
        {"seq": 4, "type": DBTCORE, "slug": "dbt-run"},
        {"seq": 1, "type": SHELLOPERATION, "slug": "git-pull"},
        {"seq": 2, "type": DBTCORE, "slug": "dbt-clean"},
        {"seq": 3, "type": DBTCORE, "slug": "dbt-deps"},
    ]
    config = {"tasks": tasks_out_of_order}

    deployment_schedule_flow_v5.fn(config)

    sorted_tasks = mock_sequential.call_args.args[0]
    assert [t["seq"] for t in sorted_tasks] == [1, 2, 3, 4]
    mock_tolerant.assert_not_called()


@patch("proxy.prefect_flows_runner._run_tasks_sequentially")
@patch("proxy.prefect_flows_runner._run_tasks_with_sync_tolerance")
def test_deployment_schedule_flow_picks_tolerance_mode_from_flag(mock_tolerant, mock_sequential):
    """continue_on_sync_failure=True dispatches to the tolerant runner. Feature
    flag must actually gate behavior — silently ignoring it means users who
    opted in still fail-fast on sync errors."""
    config = {
        "tasks": [{"seq": 1, "type": AIRBYTECONNECTION, "slug": "airbyte-sync"}],
        "continue_on_sync_failure": True,
    }
    deployment_schedule_flow_v5.fn(config)
    mock_tolerant.assert_called_once()
    mock_sequential.assert_not_called()


# =============================================================================
# shellopjob — git-pull secret block injection
# =============================================================================


@patch("proxy.prefect_flows_runner.ShellOperation")
@patch("proxy.prefect_flows_runner.Secret")
def test_shellopjob_git_pull_appends_secret_url(mock_secret, mock_shell_cls):
    """git-pull commands must be rewritten to include the secret block's URL.
    Without this, git-pull auth fails and dbt runs on stale code."""
    mock_secret.load.return_value.get.return_value = "https://oauth2:TOKEN@github.com/org/repo"

    task_config = {
        "slug": "git-pull",
        "commands": ["git pull"],
        "env": {"secret-git-pull-url-block": "gh-secret", "shell": "/bin/bash"},
        "working_dir": "/tmp/dbt",
    }
    shellopjob.fn(task_config, "git-pull")

    # ShellOperation.run() got the URL-appended command
    called_cmds = mock_shell_cls.call_args.kwargs["commands"]
    assert called_cmds == ["git pull https://oauth2:TOKEN@github.com/org/repo"]
