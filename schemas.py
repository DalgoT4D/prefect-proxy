"""Schemas for requests"""

from pydantic import BaseModel


class AirbyteServerCreate(BaseModel):
    """payload to create an airbyte server block"""

    blockName: str
    serverHost: str
    serverPort: str
    apiVersion: str


class AirbyteConnectionCreate(BaseModel):
    """payload to create an airbyte connection block"""

    serverBlockName: str
    connectionId: str
    connectionBlockName: str


class PrefectShellSetup(BaseModel):
    """payload to create a shell block"""

    blockName: str
    commands: list
    workingDir: str
    env: dict


class DbtProfileCreate(BaseModel):
    """this is part of the dbt block creation payload"""

    name: str
    target: str
    target_configs_schema: str


class DbtCoreCreate(BaseModel):
    """payload to create a dbt core command block"""

    blockName: str

    profile: DbtProfileCreate
    wtype: str
    credentials: dict

    commands: list
    env: dict
    working_dir: str
    profiles_dir: str
    project_dir: str


class RunFlow(BaseModel):
    """just a blockname"""

    blockName: str


class DeploymentCreate(BaseModel):
    """parameters to create a deployment from a flow"""

    flow_name: str
    deployment_name: str
    org_slug: str
    connection_blocks: list
    dbt_blocks: list
    cron: str
