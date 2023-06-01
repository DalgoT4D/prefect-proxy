"""Schemas for requests"""

from uuid import UUID
from pydantic import BaseModel


class AirbyteServerCreate(BaseModel):
    """payload to create an airbyte server block"""

    blockName: str
    serverHost: str
    serverPort: str
    apiVersion: str


class AirbyteServerBlockResponse(BaseModel):
    """response from the airbyte server block"""

    block_id: str


class AirbyteConnectionCreate(BaseModel):
    """payload to create an airbyte connection block"""

    serverBlockName: str
    connectionId: str
    connectionBlockName: str


class AirbyteConnectionBlockResponse(BaseModel):
    """response from the airbyte connection block"""

    block_id: str


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


class DbtCliProfile(BaseModel):
    """this is part of the dbt block creation payload"""

    name: str
    target: str
    target_configs: str


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


class DbtCoreBlockResponse(BaseModel):
    """response from the dbt block"""

    block_id: str


class DeploymentSchema(BaseModel):
    """this is part of the deployment block creation payload"""

    id: UUID
    name: str


class PostDeploymentResponse(BaseModel):
    """response from the post deployment block"""

    deployment: DeploymentSchema


class RunFlow(BaseModel):
    """just a blockname"""

    blockName: str
    flowName: str = None
    flowRunName: str = None


class FlowRunsResponse(BaseModel):
    """response from the flow runs block"""

    flow_runs: list


class DeploymentCreate(BaseModel):
    """parameters to create a deployment from a flow"""

    flow_name: str
    deployment_name: str
    org_slug: str
    connection_blocks: list
    dbt_blocks: list
    cron: str = None


class DeploymentFetch(BaseModel):
    """parameters to filter deployments by while fetching"""

    org_slug: str
    deployment_ids: list[str] | None


class FlowRunRequest(BaseModel):
    """search flow runs"""

    name: str
