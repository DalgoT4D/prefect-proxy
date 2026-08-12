"""Schemas for requests"""

from uuid import UUID
from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Literal, Optional


class AirbyteServerCreate(BaseModel):
    """payload to create an airbyte server block"""

    blockName: str
    serverHost: str
    serverPort: str
    apiVersion: str
    useSSL: bool = False


class AirbyteServerUpdate(BaseModel):
    """payload to create an airbyte server block"""

    blockName: str
    serverHost: Optional[str] = None
    serverPort: Optional[str] = None
    apiVersion: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    useSSL: Optional[bool] = None


class AirbyteServerBlockResponse(BaseModel):
    """response from the airbyte server block"""

    block_id: str


class AirbyteConnectionCreate(BaseModel):
    """payload to upsert an airbyte connection block"""

    serverBlockName: str
    connectionId: str
    connectionBlockName: str
    connectionName: str = ""
    extra: dict = {}


class AirbyteConnectionBlockResponse(BaseModel):
    """response from the airbyte connection block"""

    block_id: str


class PrefectShellSetup(BaseModel):
    """payload to create a shell block"""

    blockName: str
    commands: list
    workingDir: str
    env: dict


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
    flowName: Optional[str] = None
    flowRunName: Optional[str] = None


class RunDbtCoreOperation(BaseModel):
    """config payload to run a dbt core operation: clean, deps, test"""

    type: str
    slug: str
    profiles_dir: str
    project_dir: str
    working_dir: str
    env: dict
    commands: list
    cli_profile_block: str
    cli_args: list = []
    flow_name: str
    flow_run_name: str


class RunShellOperation(BaseModel):
    """config payload to run a shell operation in prefect"""

    type: str
    slug: str
    commands: list
    working_dir: str
    env: dict
    flow_name: str
    flow_run_name: str


class RunAirbyteResetConnection(BaseModel):
    """config payload to reset an airbyte connection"""

    type: str
    slug: str
    airbyte_server_block: str
    connection_id: str
    timeout: int
    flow_name: str
    flow_run_name: str
    work_queue_name: str
    work_pool_name: str
    org_slug: str


class FlowRunsResponse(BaseModel):
    """response from the flow runs block"""

    flow_runs: list


class DeploymentCreate2(BaseModel):
    """parameters to create a deployment from a flow; going away with blocks"""

    flow_name: str
    deployment_name: str
    org_slug: str
    deployment_params: dict
    cron: Optional[str] = None
    work_queue_name: Optional[str] = None
    work_pool_name: Optional[str] = None


class DeploymentUpdate2(BaseModel):
    """parameters to create a deployment from a flow"""

    deployment_params: dict = {}
    cron: Optional[str] = None
    work_queue_name: Optional[str] = None
    work_pool_name: Optional[str] = None


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


class PrefectSecretBlockEdit(BaseModel):
    """Schema for editing a block to store a secret string in prefect"""

    secret: str
    blockName: str


class RetryFlowRunRequest(BaseModel):
    """Schema for retrying a flow run"""

    minutes: int


class ScheduleFlowRunRequest(BaseModel):
    """Schema for scheduling a flow run at a later stage"""

    runParams: dict
    scheduledTime: Optional[datetime] = None  # by default it will be scheduled to run now


class CancelQueuedManualJob(BaseModel):
    """Payload to cancel a manually queued job"""

    class State(BaseModel):
        name: str
        type: Literal["CANCELLING"]

    state: State
    force: str

    model_config = ConfigDict(from_attributes=True)


class FilterLateFlowRuns(BaseModel):
    """Filter late flow runs"""

    deployment_id: Optional[str] = None
    work_pool_name: Optional[str] = None
    work_queue_name: Optional[str] = None
    limit: int = 1
    before_start_time: Optional[datetime] = None
    after_start_time: Optional[datetime] = None
    exclude_flow_run_ids: list[str] = []


class FilterPrefectWorkers(BaseModel):
    """FIlter prefect workers"""

    work_queue_names: list[str] = []
    work_pool_names: list[str] = []
    status: str = "ONLINE"
