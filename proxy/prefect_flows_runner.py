"""
Runner-based flow file. Self-contained: no imports from prefect_flows.py so the
old file can be retired once every deployment has been cut over.

Migrates DBTCORE execution off the deprecated DbtCoreOperation / DbtCliProfile
blocks onto PrefectDbtRunner, which uses the worker's base-env dbt-core (pinned
to 1.10.19 in pyproject.toml and the runner Dockerfile).

Cutover per deployment is a Prefect-DB entrypoint rewrite from
    proxy/prefect_flows.py:deployment_schedule_flow_v4
to
    proxy/prefect_flows_runner.py:deployment_schedule_flow_v5
Rollback = reverse the rewrite. The old flow file stays intact so rollback is safe.

Notable differences from prefect_flows.py:
  - dbt: PrefectDbtRunner + Secret block (was DbtCoreOperation + CLI block)
  - shell: non-deprecated prefect_shell.commands.ShellOperation
  - dbt / shell / airbyte tasks are @flow (not @task) so their internal
    nodes surface as subflows in the graph
"""

import asyncio
import json
import os
import shlex
from datetime import datetime
from pathlib import Path
from time import sleep

import yaml
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.states import State, StateType
from prefect_airbyte import AirbyteConnection, AirbyteServer
from prefect_airbyte.flows import (
    clear_connection,
    clear_connection_streams,
    run_connection_sync,
    update_connection_schema,
)
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run
from prefect_shell.commands import ShellOperation

from proxy.helpers import CustomLogger

logger = CustomLogger("prefect-proxy")


# Django prefect block-type names (must match backend constants).
AIRBYTESERVER = "Airbyte Server"
AIRBYTECONNECTION = "Airbyte Connection"
SHELLOPERATION = "Shell Operation"
DBTCORE = "dbt Core Operation"
DBTCLOUD = "dbt Cloud Job"


# =============================================================================
# Airbyte flows
# =============================================================================
# Copied from prefect_flows.py. Same function names/signatures/bodies. Only
# difference: `flow_run_name` set on the decorator so the graph label is stable
# (Prefect's default is a random <adj>-<animal>).


@flow(flow_run_name="airbyte-sync-trigger", retries=1, retry_delay_seconds=120)
def run_airbyte_connection_flow_v1(payload: dict):
    """run an airbyte sync"""
    try:
        serverblock = AirbyteServer.load(payload["airbyte_server_block"])
        connection_block = AirbyteConnection(
            airbyte_server=serverblock,
            connection_id=payload["connection_id"],
            timeout=payload["timeout"] or 15,
        )
        # Rename the nested prefect_airbyte subflow so it doesn't show up as a
        # random <adj>-<animal> in the graph.
        result = run_connection_sync.with_options(flow_run_name="airbyte-sync")(
            connection_block
        )
        logger.info("airbyte connection sync result=")
        logger.info(result)
        return result
    except Exception as error:  # pylint: disable=broad-exception-caught
        logger.error(str(error))
        raise


@flow(flow_run_name="airbyte-clear-trigger")
def run_airbyte_conn_clear(payload: dict):
    """reset an airbyte connection"""
    try:
        serverblock = AirbyteServer.load(payload["airbyte_server_block"])
        connection_block = AirbyteConnection(
            airbyte_server=serverblock,
            connection_id=payload["connection_id"],
            timeout=payload["timeout"] or 15,
        )
        if "streams" in payload and payload["streams"]:
            result = clear_connection_streams.with_options(
                flow_run_name="airbyte-clear-streams"
            )(connection_block, payload["streams"])
        else:
            result = clear_connection.with_options(flow_run_name="airbyte-clear")(
                connection_block
            )
        logger.info("airbyte connection clear result=")
        logger.info(result)
        return result
    except Exception as error:  # pylint: disable=broad-exception-caught
        logger.error(str(error))
        raise


@flow(flow_run_name="airbyte-update-schema-trigger")
async def run_refresh_schema_flow(payload: dict, catalog_diff: dict):
    """refresh an airbyte connection's schema"""
    try:
        serverblock = await AirbyteServer.aload(payload["airbyte_server_block"])
        connection_block = AirbyteConnection(
            airbyte_server=serverblock,
            connection_id=payload["connection_id"],
            timeout=max(payload.get("timeout", 0), 100),
        )
        await update_connection_schema.with_options(flow_run_name="airbyte-update-schema")(
            connection_block, catalog_diff=catalog_diff
        )
        return True
    except Exception as error:  # pylint: disable=broad-exception-caught
        logger.error(str(error))
        raise


# =============================================================================
# dbt runner flow
# =============================================================================

# Fixed label used inside profiles.yml. dbt reads `target:` from the profile to
# pick which output to use — since we write exactly one output, the label is
# arbitrary but must be consistent between profiles.yml keys and PrefectDbtSettings.
DBT_TARGET = "default"


def _read_profile_name(project_dir: str) -> str:
    """Read the `profile:` key from dbt_project.yml. profiles.yml's top-level
    key must match this so dbt can resolve the profile.
    """
    with open(Path(project_dir) / "dbt_project.yml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["profile"]


def _build_output(wtype: str, schema: str, creds: dict, extras: dict, threads: int) -> dict:
    """Inner `outputs.<target>` dict for postgres or bigquery."""
    if wtype == "postgres":
        # airbyte spec calls it "username"; dbt-postgres wants "user"
        pg_creds = dict(creds)
        if "username" in pg_creds and "user" not in pg_creds:
            pg_creds["user"] = pg_creds.pop("username")
        # airbyte's `schema` in creds is the destination schema; profiles.yml's
        # `schema` is dbt's target schema (from OrgDbt.default_schema). Drop the
        # cred version so it doesn't override.
        pg_creds.pop("schema", None)
        # sslrootcert_content is written to disk by the runner (see dbtjob_v2_runner)
        # and referenced by `sslrootcert`. It's not a valid dbt field itself.
        pg_creds.pop("sslrootcert_content", None)
        output = {"type": "postgres", "schema": schema, "threads": threads}
        output.update(pg_creds)
        output.update(extras)
        return output

    if wtype == "bigquery":
        output = {
            "type": "bigquery",
            "schema": schema,
            "threads": threads,
            "method": "service-account-json",
            "keyfile_json": creds,
        }
        if "location" in extras:
            output["location"] = extras["location"]
        if "priority" in extras:
            output["priority"] = extras["priority"]
        return output

    raise ValueError(f"Unsupported warehouse type: {wtype}")


def build_profile_dict(
    *,
    profile_name: str,
    wtype: str,
    target: str,
    schema: str,
    creds: dict,
    extras: dict,
    threads: int = 4,
) -> dict:
    """Build a complete profiles.yml dict — single profile, single output.

    <profile_name>:
      target: <target>
      outputs:
        <target>: {type, schema, threads, ...creds & extras}
    """
    return {
        profile_name: {
            "target": target,
            "outputs": {target: _build_output(wtype, schema, creds, extras, threads)},
        }
    }


@flow(
    name="dbtjob_v2_runner",
    flow_run_name="dbtjob-{task_slug}",
    retries=1,
    retry_delay_seconds=60,
)
def dbtjob_v2_runner(task_config: dict, task_slug: str):  # pylint: disable=unused-argument
    """Run dbt commands via PrefectDbtRunner. Reads the dbt-profile Secret block
    at flow-run start, writes a resolved profiles.yml to the worker's filesystem,
    then invokes each dbt command as argv.

    Block value shape (JSON-encoded):
      {"wtype": ..., "default_schema": ..., "creds": {...}, "extras": {...}}

    Runs as a subflow (not a task) so that PrefectDbtRunner's per-node tasks
    (model / test / seed / snapshot) nest under this subflow in the graph
    instead of surfacing at the top-level deployment flow.

    Postgres SSL cert content (if present in creds.sslrootcert_content) is
    written to disk at creds.sslrootcert (backend-computed path) or a fallback
    next to profiles.yml; the output's `sslrootcert` field is rewritten to
    point at that path.
    """
    env = task_config["env"]
    raw = Secret.load(env["dbt-profile-secret-block"]).get()
    # Prefect stores block values in a JSON column; a JSON-valid string round-trips
    # back as a dict on .get(). Accept both shapes.
    block_value = raw if isinstance(raw, dict) else json.loads(raw)
    wtype = block_value["wtype"]
    default_schema = block_value["default_schema"]
    creds = block_value["creds"]
    extras = block_value.get("extras", {})

    profile_name = _read_profile_name(task_config["project_dir"])
    profile_dict = build_profile_dict(
        profile_name=profile_name,
        wtype=wtype,
        target=DBT_TARGET,
        schema=default_schema,
        creds=creds,
        extras=extras,
    )

    profiles_dir = Path(task_config["profiles_dir"])
    profiles_dir.mkdir(parents=True, exist_ok=True)

    # SSL cert content for postgres travels inside creds. Match the old
    # dbtjob_v1 path exactly (prefect_flows.py:200-209): prefer creds["sslrootcert"]
    # (backend already sets this to <org_project_dir>/sslrootcert.pem), fall back
    # to <project_dir>/../sslrootcert.pem — same resolved location.
    if wtype == "postgres" and creds.get("sslrootcert_content"):
        cert_path = creds.get("sslrootcert") or os.path.join(
            task_config["project_dir"], "..", "sslrootcert.pem"
        )
        os.makedirs(os.path.dirname(cert_path), exist_ok=True)
        with open(cert_path, "w", encoding="utf-8") as f:
            f.write(creds["sslrootcert_content"])
        profile_dict[profile_name]["outputs"][DBT_TARGET]["sslrootcert"] = cert_path

    (profiles_dir / "profiles.yml").write_text(yaml.safe_dump(profile_dict))

    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=task_config["project_dir"],
            profiles_dir=str(profiles_dir),
        )
    )

    # task_config["commands"] arrives as shell strings prefixed with the org's
    # dbt binary path (e.g. "/home/ddp/dbt/venv/bin/dbt run --full-refresh").
    # PrefectDbtRunner uses the worker's own dbt-core, so we drop the binary
    # token and pass the rest as argv.
    result = None
    for cmd in task_config["commands"]:
        argv = shlex.split(cmd)[1:]
        try:
            result = runner.invoke(argv)
        except Exception:  # pylint: disable=broad-exception-caught
            if task_config["slug"] == "dbt-test":
                return State(
                    type=StateType.COMPLETED,
                    name="DBT_TEST_FAILED",
                    message="WARNING: dbt test failed",
                )
            raise
    return result


# =============================================================================
# Shell operation flow
# =============================================================================
# Copied from prefect_flows.py::shellopjob. Two differences:
#  - Now a @flow (was @task) — matches dbtjob_v2_runner so all deployment nodes
#    surface as subflows in the graph.
#  - Uses non-deprecated `prefect_shell.commands.ShellOperation` instead of the
#    deprecated `prefect_dbt.cli.commands.ShellOperation`.


@flow(name="shellopjob", flow_run_name="shellop-{task_slug}")
def shellopjob(task_config: dict, task_slug: str):  # pylint: disable=unused-argument
    """loads and runs the shell operation"""

    if task_config["slug"] == "git-pull":
        secret_block_name = task_config["env"].get("secret-git-pull-url-block", "")
        git_repo_endpoint = ""
        if secret_block_name and len(secret_block_name) > 0:
            secret_blk = Secret.load(secret_block_name)
            git_repo_endpoint = secret_blk.get()

        commands = task_config["commands"]
        updated_cmds = [f"{cmd} {git_repo_endpoint}" for cmd in commands]
        task_config["commands"] = updated_cmds

    elif task_config["slug"] == "git-clone":
        secret_block_name = task_config["env"].get("secret-git-pull-url-block", "")
        project_dir = task_config["env"].get("project_dir", "")

        git_repo_endpoint = task_config["env"].get("gitrepo_url", "")
        if secret_block_name and len(secret_block_name) > 0:
            secret_blk = Secret.load(secret_block_name)
            git_repo_endpoint = secret_blk.get()

        if not git_repo_endpoint:
            raise ValueError(
                "Git repository endpoint is not provided in the environment variables or secret block."
            )

        commands = task_config["commands"]
        updated_cmds = [f"{cmd} {git_repo_endpoint} {project_dir}" for cmd in commands]
        task_config["commands"] = updated_cmds

    elif task_config["slug"] == "generate-edr":
        # Single consolidated Secret block replaces the old trio
        # (edr-aws-access-key / edr-aws-access-secret / edr-s3-bucket).
        # Backend scaffolds it via `manage.py sync_edr_secret_block`.
        raw = Secret.load("edr-s3-creds").get()
        edr_config = raw if isinstance(raw, dict) else json.loads(raw)
        aws_access_key = edr_config["aws_access_key_id"]
        aws_access_secret = edr_config["aws_secret_access_key"]
        edr_s3_bucket = edr_config["s3_bucket"]
        todays_date = datetime.today().strftime("%Y-%m-%d")
        task_config["commands"][0] = task_config["commands"][0].replace("TODAYS_DATE", todays_date)
        task_config["commands"][0] = task_config["env"]["PATH"] + "/" + task_config["commands"][0]
        task_config["commands"][0] += (
            f" --aws-access-key-id {aws_access_key}"
            f" --aws-secret-access-key {aws_access_secret}"
            f" --s3-bucket-name {edr_s3_bucket}"
        )

    shell_op = ShellOperation(
        commands=task_config["commands"],
        working_dir=task_config["working_dir"],
        shell=(task_config["env"]["shell"] if "shell" in task_config["env"] else "/bin/bash"),
    )
    return shell_op.run()


# =============================================================================
# ad-hoc entrypoints
# =============================================================================
# For ad-hoc UI Run-button invocations, main.py calls the task flows
# (`dbtjob_v2_runner`, `shellopjob`) directly — no wrapper subflow, so the
# graph shows only the actual work, not two nested rows.


# =============================================================================
# dbt cloud task
# =============================================================================


@task(name="dbtcloudjob_v1", task_run_name="dbtcloudjob-{task_slug}")
async def dbtcloudjob_v1(task_config: dict, task_slug: str):  # pylint: disable=unused-argument
    """trigger a dbt Cloud job run"""
    try:
        dbt_cloud_creds = await DbtCloudCredentials.aload(task_config["dbt_cloud_creds_block"])
        result = await trigger_dbt_cloud_job_run(dbt_cloud_creds, task_config["dbt_cloud_job_id"])
        return result
    except Exception as error:  # pylint: disable=broad-exception-caught
        logger.error(str(error))
        raise


# =============================================================================
# dispatcher + deployment entrypoint
# =============================================================================


def _is_airbyte_sync_task(task_config: dict) -> bool:
    """Check if a task is an airbyte sync task"""
    return task_config["type"] == AIRBYTECONNECTION and task_config["slug"] == "airbyte-sync"


def _run_task_runner(task_config: dict):
    """Copy of prefect_flows._run_task with DBTCORE and AIRBYTECONNECTION
    branches dispatching to local runner-file versions; DBTCLOUD and
    SHELLOPERATION still delegate to prefect_flows.
    """
    if task_config["type"] == DBTCORE:
        dbtjob_v2_runner(task_config, task_config["slug"])

    elif task_config["type"] == DBTCLOUD:
        asyncio.run(dbtcloudjob_v1(task_config, task_config["slug"]))

    elif task_config["type"] == SHELLOPERATION:
        shellopjob(task_config, task_config["slug"])

    elif task_config["type"] == AIRBYTECONNECTION:
        if task_config["slug"] == "airbyte-sync":
            run_airbyte_connection_flow_v1(task_config)

        elif task_config["slug"] == "airbyte-clear":
            run_airbyte_conn_clear(task_config)

        elif task_config["slug"] == "update-schema":
            asyncio.run(
                run_refresh_schema_flow(
                    task_config, catalog_diff=task_config.get("catalog_diff", {})
                )
            )
        else:
            raise ValueError(f"Unsupported AIRBYTECONNECTION slug: {task_config['slug']}")

    else:
        raise ValueError(f"Unknown task type: {task_config['type']}")


def _run_tasks_sequentially(tasks: list):
    """Sequential execution, fail-fast."""
    try:
        for task_config in tasks:
            _run_task_runner(task_config)
            sleep(30)
    except Exception as error:  # pylint: disable=broad-exception-caught
        logger.exception(error)
        raise


def _run_tasks_with_sync_tolerance(tasks: list):
    """Airbyte syncs first (collect errors), then transforms sequentially."""
    sync_tasks = [t for t in tasks if _is_airbyte_sync_task(t)]
    other_tasks = [t for t in tasks if not _is_airbyte_sync_task(t)]

    sync_errors = []
    for task_config in sync_tasks:
        try:
            _run_task_runner(task_config)
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error(
                "Airbyte sync failed for connection %s: %s",
                task_config.get("connection_id"),
                error,
            )
            sync_errors.append(error)
        sleep(30)

    if sync_errors:
        raise RuntimeError(
            f"{len(sync_errors)} airbyte sync(s) failed: " + "; ".join(str(e) for e in sync_errors)
        )

    try:
        for task_config in other_tasks:
            _run_task_runner(task_config)
            sleep(30)
    except Exception as error:  # pylint: disable=broad-exception-caught
        logger.exception(error)
        raise


@flow
def deployment_schedule_flow_v5(
    config: dict,
    dbt_blocks: list | None = None,  # pylint: disable=unused-argument
    airbyte_blocks: list | None = None,  # pylint: disable=unused-argument
):
    """Runner-based deployment entrypoint. Cutover per deployment via a Prefect
    DB rewrite of the `entrypoint` field to
    `proxy/prefect_flows_runner.py:deployment_schedule_flow_v5`.
    """
    config["tasks"].sort(key=lambda blk: blk["seq"])
    if config.get("continue_on_sync_failure"):
        _run_tasks_with_sync_tolerance(config["tasks"])
    else:
        _run_tasks_sequentially(config["tasks"])
