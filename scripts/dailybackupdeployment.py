import json
from prefect import flow, task
from prefect.blocks.system import Secret

# from prefect.tasks import ShellTask # for prefect 3
from prefect_shell import ShellOperation


@task
def backup_one_rds(rds_to_backup):
    """
    looks up the secret for the given rds
    extracts the environment variables from the secret
    and runs the backup script with that environment
    """
    print(f"Backing up {rds_to_backup}")

    secret_block = Secret.load(rds_to_backup)
    secret_value = secret_block.get()
    secret_env = json.loads(secret_value)

    # Run the script with the secret as an environment variable
    ShellOperation(
        commands=["/home/ddp/maintenance/.venv/bin/python /home/ddp/maintenance/dailybackup.py"],
        env=secret_env,
        working_dir="/home/ddp/maintenance",
    ).run()


@flow
def backup_production():
    """backs up the production database"""
    backup_one_rds("dailybackup-production")


@flow
def backup_superset():
    """backs up the superset database"""
    backup_one_rds("dailybackup-superset")


@flow
def backup_airbyte_prod():
    """backs up the airbyte-prod database"""
    backup_one_rds("dailybackup-airbyte-prod")


@flow
def backup_superset_shofco():
    """backs up the superset-shofco database"""
    backup_one_rds("dailybackup-superset-shofco")


@flow
def backup_staging():
    """backs up the staging database"""
    backup_one_rds("dailybackup-staging")


@flow
def backup_staging_warehouses():
    """backs up the staging-warehouses database"""
    backup_one_rds("dailybackup-staging-warehouses")
