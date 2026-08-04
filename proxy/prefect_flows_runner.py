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
import re
import shlex
import subprocess
import sys
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


@task(name="post-sync-ops", retries=0)
async def _run_post_sync_ops(env: dict, ops: list) -> None:
    """Execute post-sync operations (e.g. type casts) after an Airbyte sync.
    No-op when ops is absent or empty."""
    post_sync_ops = ops
    if not post_sync_ops:
        return

    block_name = env.get("dbt-profile-secret-block")
    if not block_name:
        logger.error("post_sync_ops present but no dbt-profile-secret-block in env — skipping")
        return

    secret = await Secret.aload(block_name)
    raw = secret.get()
    data = raw if isinstance(raw, dict) else json.loads(raw)
    wtype = data["wtype"]
    creds = data["creds"]

    for op in post_sync_ops:
        if op.get("type") != "cast":
            continue
        sql = op["sql"]

        if wtype == "postgres":
            import psycopg2  # available via dbt-postgres in pyproject.toml

            conn = psycopg2.connect(
                host=creds["host"],
                port=creds.get("port", 5432),
                dbname=creds["database"],
                user=creds["user"],
                password=creds["password"],
            )
            try:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(sql)
                logger.info("post-sync cast executed (postgres)")
            finally:
                conn.close()

        elif wtype == "bigquery":
            from google.oauth2.service_account import Credentials
            from google.cloud import bigquery as bq

            credentials = Credentials.from_service_account_info(creds)
            client = bq.Client(credentials=credentials, project=creds["project_id"])
            try:
                client.query(sql).result()
                logger.info("post-sync cast executed (bigquery)")
            finally:
                client.close()

        else:
            logger.error("_run_post_sync_ops: unsupported wtype=%s — skipping op", wtype)


@flow(flow_run_name="airbyte-sync-trigger", retries=1, retry_delay_seconds=120)
async def run_airbyte_connection_flow_v1(payload: dict):
    """run an airbyte sync"""
    connection_id = payload["connection_id"]

    # Try loading the persisted AirbyteConnection block (contains post-sync ops in .extra).
    # If it doesn't exist, fall back to inline construction — connections without cast
    # config never have a block, and the flow should still work.
    try:
        connection_block = await AirbyteConnection.aload(connection_id)
    except ValueError:
        serverblock = await AirbyteServer.aload(payload["airbyte_server_block"])
        connection_block = AirbyteConnection(
            airbyte_server=serverblock,
            connection_id=connection_id,
            timeout=payload["timeout"] or 15,
        )

    try:
        result = await run_connection_sync.with_options(flow_run_name="airbyte-sync")(
            connection_block
        )
        logger.info("airbyte connection sync result=")
        logger.info(result)
    except Exception as error:  # pylint: disable=broad-exception-caught
        logger.error(str(error))
        raise

    try:
        extra = connection_block.extra or {}
        await _run_post_sync_ops(
            env=extra.get("env", {}),
            ops=extra.get("post_sync_ops", []),
        )
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.error("post-sync ops failed (sync already succeeded): %s", err)
    return result


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
            result = clear_connection_streams.with_options(flow_run_name="airbyte-clear-streams")(
                connection_block, payload["streams"]
            )
        else:
            result = clear_connection.with_options(flow_run_name="airbyte-clear")(connection_block)
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
        # `project` (dbt alias for `database`) is required at the top level;
        # if omitted, dbt-bigquery falls back to google.auth.default() (ADC).
        output = {
            "type": "bigquery",
            "schema": schema,
            "threads": threads,
            "method": "service-account-json",
            "project": creds.get("project_id"),
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
    """Run dbt commands via ShellOperation. Reads the dbt-profile Secret block
    at flow-run start, writes a resolved profiles.yml to the worker's filesystem,
    then runs each dbt command as a subprocess.

    Block value shape (JSON-encoded):
      {"wtype": ..., "default_schema": ..., "creds": {...}, "extras": {...}}

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

    dbt_bin = os.path.join(os.path.dirname(sys.executable), "dbt")
    project_dir = task_config["project_dir"]

    # task_config["commands"] arrives as shell strings prefixed with the org's
    # dbt binary path (e.g. "/home/ddp/dbt/venv/bin/dbt run --full-refresh").
    # We drop the binary token and substitute the worker's own dbt binary,
    # then append --profiles-dir and --project-dir explicitly.
    for cmd in task_config["commands"]:
        argv = shlex.split(cmd)[1:]
        full_cmd = shlex.join(
            [dbt_bin, *argv, "--profiles-dir", str(profiles_dir), "--project-dir", project_dir]
        )
        try:
            ShellOperation(
                commands=[full_cmd],
                working_dir=project_dir,
            ).run()
        except Exception:  # pylint: disable=broad-exception-caught
            if task_config["slug"] == "dbt-test":
                return State(
                    type=StateType.COMPLETED,
                    name="DBT_TEST_FAILED",
                    message="WARNING: dbt test failed",
                )
            raise


# =============================================================================
# Elementary profile builder (used by generate-edr)
# =============================================================================
# Mirrors DDP_backend/ddpui/ddpdbt/elementary_service.py:create_elementary_profile.
# We reimplement here because the runner runs on Prefect workers (potentially
# EKS pods) that can't import Django code.


_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(text: str) -> str:
    return _ANSI_ESCAPE_RE.sub("", text)


def _extract_elementary_profile_from_macro_output(lines: list[str]) -> dict:
    """The elementary macro emits its YAML profile starting with `elementary:`
    somewhere in stdout after logging noise. Buffer from that marker onwards
    and parse. Raises if the marker never appears."""
    buffer = ""
    gather = False
    for line in lines:
        clean = _strip_ansi(line)
        if clean == "elementary:":
            gather = True
        if gather:
            # A non-empty, non-indented line after the first means we've hit
            # a dbt log/warning line — the YAML block is done.
            if buffer and clean and not clean[0].isspace():
                break
            buffer += clean + "\n"
    if not buffer:
        raise RuntimeError(
            "elementary.generate_elementary_cli_profile macro returned no profile — "
            "check that the elementary package is installed (dbt deps ran)."
        )
    return yaml.safe_load(buffer)


def _prepare_elementary_profile(working_dir: str, dbt_profile_secret_block_name: str) -> None:
    """Generate <working_dir>/elementary_profiles/profiles.yml at flow-run time.

    Self-contained — does NOT assume any prior task has written profiles.yml
    or run `dbt deps`. Works for both "EDR at end of pipeline" (safe overwrite)
    and standalone EDR (regenerate button on a fresh EKS pod).

    Precondition: dbt project on disk at working_dir (git-clone step earlier
    in the pipeline). Everything else is generated here.

    Flow (mirrors DDP_backend:create_elementary_profile):
      1. Load dbt-profile Secret block → creds/wtype/default_schema/extras.
      2. Read dbt_project.yml for the profile name.
      3. Write <working_dir>/profiles/profiles.yml (same shape as dbtjob_v2_runner).
      4. `dbt deps` — ensures the elementary dbt package is in dbt_packages/.
      5. Run `dbt run-operation elementary.generate_elementary_cli_profile`
         to compute the elementary schema (dbt's schema-generation rules
         handle target-specific / config-specific quirks).
      6. Parse macro output → elementary_profile dict with computed schema.
      7. Build elementary output: copy dbt's warehouse creds, override schema.
      8. Write <working_dir>/elementary_profiles/profiles.yml.
    """
    project_dir = Path(working_dir)

    # 1. Secret block → creds + schema + wtype + extras.
    raw = Secret.load(dbt_profile_secret_block_name).get()
    block_value = raw if isinstance(raw, dict) else json.loads(raw)
    wtype = block_value["wtype"]
    default_schema = block_value["default_schema"]
    creds = block_value["creds"]
    extras = block_value.get("extras", {})

    # 2. Profile name from dbt_project.yml.
    with open(project_dir / "dbt_project.yml", encoding="utf-8") as f:
        dbt_project = yaml.safe_load(f)
    dbt_profile_name = dbt_project["profile"]

    # 3. Write profiles.yml (same shape dbtjob_v2_runner writes).
    profile_dict = build_profile_dict(
        profile_name=dbt_profile_name,
        wtype=wtype,
        target=DBT_TARGET,
        schema=default_schema,
        creds=creds,
        extras=extras,
    )
    # SSL cert content (postgres) — write to disk + rewrite path (parity with
    # dbtjob_v2_runner). See that function for the cert-path selection logic.
    if wtype == "postgres" and creds.get("sslrootcert_content"):
        cert_path = creds.get("sslrootcert") or os.path.join(
            str(project_dir), "..", "sslrootcert.pem"
        )
        os.makedirs(os.path.dirname(cert_path), exist_ok=True)
        with open(cert_path, "w", encoding="utf-8") as f:
            f.write(creds["sslrootcert_content"])
        profile_dict[dbt_profile_name]["outputs"][DBT_TARGET]["sslrootcert"] = cert_path

    profiles_dir = project_dir / "profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)
    # Add a 'prod' output (identical creds) so the elementary macro runs with
    # target.name == 'prod'. Projects that gate elementary on
    # `+enabled: "{{ target.name in ['prod'] }}"` exclude elementary models
    # from dbt's graph for any other target, causing the macro to return
    # schema: null or raise an error. Running as 'prod' keeps elementary
    # enabled without executing any models.
    profile_dict[dbt_profile_name]["outputs"]["prod"] = profile_dict[dbt_profile_name]["outputs"][
        DBT_TARGET
    ].copy()
    (profiles_dir / "profiles.yml").write_text(yaml.safe_dump(profile_dict))
    logger.info(f"wrote {profiles_dir / 'profiles.yml'}")

    # dbt binary — via the running Python's bin dir. Portable across:
    #   - EKS runner image (pip installed → /usr/local/bin)
    #   - local prefect-proxy .venv (uv installed → .venv/bin)
    dbt_bin = os.path.join(os.path.dirname(sys.executable), "dbt")

    # 4. dbt deps — installs elementary dbt package into dbt_packages/.
    # Idempotent; safe if already installed. Fails loudly if packages.yml
    # doesn't declare elementary (surface: user hasn't set up elementary yet).
    logger.info("running dbt deps")
    ShellOperation(
        commands=[f"{dbt_bin} deps --profiles-dir profiles"],
        working_dir=str(project_dir),
    ).run()

    # 5. Run the macro with --target prod so target.name == 'prod', keeping
    # elementary enabled for projects that gate it on target name.
    logger.info("running elementary.generate_elementary_cli_profile macro")
    macro_output = ShellOperation(
        commands=[
            f"{dbt_bin} run-operation elementary.generate_elementary_cli_profile"
            " --profiles-dir profiles --target prod --no-use-colors"
        ],
        working_dir=str(project_dir),
        stream_output=False,
    ).run()

    # 6. Parse macro output.
    # The macro sets `target:` to whatever --target flag was passed, but the
    # actual output key inside `outputs:` is always "default". Use the first
    # key to avoid a KeyError when target != the output key name.
    elementary_profile = _extract_elementary_profile_from_macro_output(macro_output)
    output_key = next(iter(elementary_profile["elementary"]["outputs"]))

    # BQ emits the schema under `dataset` — normalize to `schema`.
    if elementary_profile["elementary"]["outputs"][output_key]["type"] == "bigquery":
        elementary_schema = elementary_profile["elementary"]["outputs"][output_key]["dataset"]
    else:
        elementary_schema = elementary_profile["elementary"]["outputs"][output_key]["schema"]

    if elementary_schema is None:
        raise RuntimeError(
            "elementary schema resolved to null even with target 'prod'. "
            "Ensure models.elementary.+schema is set in dbt_project.yml."
        )

    # 7. Build elementary output: dbt's warehouse creds + elementary schema.
    # Always normalise to DBT_TARGET ('default') regardless of which target the
    # macro was run against. This keeps --profile-target default (stored in the
    # OrgTask) consistent with what we write here.
    dbt_output = profile_dict[dbt_profile_name]["outputs"][DBT_TARGET]
    elementary_profile["elementary"]["target"] = DBT_TARGET
    elementary_profile["elementary"]["outputs"] = {
        DBT_TARGET: {
            **dbt_output,
            "schema": elementary_schema,
        }
    }

    # 8. Write.
    elementary_profiles_dir = project_dir / "elementary_profiles"
    elementary_profiles_dir.mkdir(exist_ok=True)
    (elementary_profiles_dir / "profiles.yml").write_text(yaml.safe_dump(elementary_profile))
    logger.info(f"wrote {elementary_profiles_dir / 'profiles.yml'}")


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
    job_env = {}

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
        _prepare_elementary_profile(
            task_config["working_dir"],
            task_config["env"]["dbt-profile-secret-block"],
        )

        raw = Secret.load("edr-s3-creds").get()
        edr_config = raw if isinstance(raw, dict) else json.loads(raw)

        edr_bin = os.path.join(os.path.dirname(sys.executable), "edr")
        todays_date = datetime.today().strftime("%Y-%m-%d")
        command = task_config["commands"][0].replace("TODAYS_DATE", todays_date)
        argv = shlex.split(command)[1:]

        task_config["commands"] = [
            " ".join([edr_bin, *argv, "--s3-bucket-name", edr_config["s3_bucket"]])
        ]
        job_env = {
            "AWS_ACCESS_KEY_ID": edr_config["aws_access_key_id"],
            "AWS_SECRET_ACCESS_KEY": edr_config["aws_secret_access_key"],
        }

    shell_op = ShellOperation(
        commands=task_config["commands"],
        working_dir=task_config["working_dir"],
        shell=(task_config["env"]["shell"] if "shell" in task_config["env"] else "/bin/bash"),
        env=job_env,
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
            asyncio.run(run_airbyte_connection_flow_v1(task_config))

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
            sleep(10)
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
        sleep(10)

    if sync_errors:
        raise RuntimeError(
            f"{len(sync_errors)} airbyte sync(s) failed: " + "; ".join(str(e) for e in sync_errors)
        )

    try:
        for task_config in other_tasks:
            _run_task_runner(task_config)
            sleep(10)
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
