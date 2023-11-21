"""Schemas for requests"""

from uuid import UUID
from pydantic import BaseModel, Extra


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
    bqlocation: str = None
    credentials: dict
    cli_profile_block_name: str

    commands: list
    env: dict
    working_dir: str
    profiles_dir: str
    project_dir: str


class DbtCliProfileBlockCreate(BaseModel, extra=Extra.allow):
    """payload to create a dbt cli profile block"""

    blockName: str
    profile: DbtProfileCreate
    wtype: str
    bqlocation: str = None
    credentials: dict


class DbtCoreCredentialUpdate(BaseModel):
    """payload to update a dbt core command block's credentials"""

    blockName: str
    credentials: dict


class DbtCoreSchemaUpdate(BaseModel):
    """payload to update a dbt core command block's schema and target"""

    blockName: str
    target_configs_schema: str


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


class DeploymentUpdate(BaseModel):
    """parameters to create a deployment from a flow"""

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


class AirbyteConnectionBlocksFetch(BaseModel):
    """These parameters define the query for fetching prefect blocks"""

    block_names: list[str] | None


class PrefectBlocksDelete(BaseModel):
    """Delete each block having the block ids in the array"""

    block_ids: list[str] | None


class PrefectSecretBlockCreate(BaseModel):
    """Schema for creating a block to store a secret string in prefect"""

    secret: str
    blockName: str
