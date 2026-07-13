"""
Runner-based flow file. Migrates DBTCORE execution off the deprecated
DbtCoreOperation / DbtCliProfile blocks onto PrefectDbtRunner, which uses the
worker's base-env dbt-core (pinned to 1.10.19 in pyproject.toml and the runner
Dockerfile).

Cutover per deployment is a Prefect-DB entrypoint rewrite from
    proxy/prefect_flows.py:deployment_schedule_flow_v4
to
    proxy/prefect_flows_runner.py:deployment_schedule_flow_v5
Rollback = reverse the rewrite. The old flow file stays intact so rollback is safe.

Only DBTCORE tasks change; airbyte, dbt-cloud, and shell tasks re-import unchanged
from prefect_flows.
"""

import asyncio
import json
import shlex
from pathlib import Path
from time import sleep

import yaml
from prefect import flow
from prefect.blocks.system import Secret
from prefect.states import State, StateType
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from proxy.helpers import CustomLogger
from proxy.prefect_flows import (
    _is_airbyte_sync_task,
    dbtcloudjob_v1,
    run_airbyte_conn_clear,
    run_airbyte_connection_flow_v1,
    run_refresh_schema_flow,
    shellopjob,
    AIRBYTECONNECTION,
    DBTCLOUD,
    DBTCORE,
    SHELLOPERATION,
)

logger = CustomLogger("prefect-proxy")

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
    """Run dbt commands via PrefectDbtRunner. Reads warehouse creds from a Prefect
    Secret block at flow-run start, writes a resolved profiles.yml to the worker's
    filesystem, then invokes each dbt command as argv.

    Runs as a subflow (not a task) so that PrefectDbtRunner's per-node tasks
    (model / test / seed / snapshot) nest under this subflow in the graph
    instead of surfacing at the top-level deployment flow.

    Postgres SSL cert content (if present in extras.sslrootcert_content) is
    written to disk next to profiles.yml; the output's `sslrootcert` field is
    rewritten to point at that path.
    """
    env = task_config["env"]
    raw = Secret.load(env["warehouse-secret-block-name"]).get()
    # Prefect stores block values in a JSON column; a JSON-valid string round-trips
    # back as a dict on .get(). Accept both shapes.
    block_value = raw if isinstance(raw, dict) else json.loads(raw)
    creds = block_value["creds"]
    extras = block_value.get("extras", {})

    profile_name = _read_profile_name(task_config["project_dir"])
    profile_dict = build_profile_dict(
        profile_name=profile_name,
        wtype=env["wtype"],
        target=DBT_TARGET,
        schema=env["default-schema"],
        creds=creds,
        extras=extras,
    )

    profiles_dir = Path(task_config["profiles_dir"])
    profiles_dir.mkdir(parents=True, exist_ok=True)

    # SSL cert content for postgres travels inside creds (matches how it's stored
    # by the existing CLI-profile-block flow). Write it to disk and rewrite the
    # output's sslrootcert field to point at it.
    if env["wtype"] == "postgres" and creds.get("sslrootcert_content"):
        cert_path = profiles_dir / "sslrootcert.pem"
        cert_path.write_text(creds["sslrootcert_content"])
        profile_dict[profile_name]["outputs"][DBT_TARGET]["sslrootcert"] = str(cert_path)

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


def _run_task_runner(task_config: dict):
    """Copy of prefect_flows._run_task with the DBTCORE branch dispatching to
    dbtjob_v2_runner. Other branches delegate to the originals in prefect_flows.
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
            f"{len(sync_errors)} airbyte sync(s) failed: "
            + "; ".join(str(e) for e in sync_errors)
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
