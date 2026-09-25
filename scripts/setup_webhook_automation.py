"""
Creates (or updates) the Prefect automation that notifies Dalgo's backend
whenever a flow run changes state.

PREFECT_API_URL is read from the environment (set in .env or injected by Docker).

Usage:
    uv run python scripts/setup_webhook_automation.py \\
        --backend-webhook-url http://backend:8002/webhooks/v1/notification/ \\
        --backend-api-key <PREFECT_NOTIFICATIONS_WEBHOOK_KEY from DDP_backend/.env>
"""

import argparse
import asyncio
import uuid

from prefect.blocks.notifications import CustomWebhookNotificationBlock
from prefect.client.orchestration import get_client
from prefect.events.actions import SendNotification
from prefect.events.schemas.automations import AutomationCore, EventTrigger, Posture

BLOCK_NAME = "dalgo-custom-webhook-block"
AUTOMATION_NAME = "Notifications"
AUTOMATION_DESCRIPTION = "To inform dalgo"
NOTIFICATION_SUBJECT = "Prefect flow run notification"
NOTIFICATION_BODY = "Flow run {{ flow_run.name }} with id {{ flow_run.id }} entered state {{ flow_run.state.name }}"
FLOW_RUN_EVENTS = {
    "prefect.flow-run.Pending",
    "prefect.flow-run.Running",
    "prefect.flow-run.Completed",
    "prefect.flow-run.Failed",
    "prefect.flow-run.Cancelled",
    "prefect.flow-run.Crashed",
    "prefect.flow-run.TimedOut",
}


async def setup_block(webhook_url: str, api_key: str) -> uuid.UUID:
    block = CustomWebhookNotificationBlock(
        name="Dalgo webhook",
        url=webhook_url,
        method="POST",
        headers={"X-Notification-Key": api_key},
        json_data={"body": "{{body}}"},
        timeout=30.0,
    )
    doc_id = await block.save(BLOCK_NAME, overwrite=True)
    print(f"  Block document saved ({doc_id}).")
    return doc_id


async def setup_automation(client, block_document_id: uuid.UUID) -> uuid.UUID:
    automation = AutomationCore(
        name=AUTOMATION_NAME,
        description=AUTOMATION_DESCRIPTION,
        enabled=True,
        tags=[],
        trigger=EventTrigger(
            match={"prefect.resource.id": "prefect.flow-run.*"},
            expect=FLOW_RUN_EVENTS,
            for_each={"prefect.resource.id"},
            posture=Posture.Reactive,
            threshold=1,
        ),
        actions=[
            SendNotification(
                block_document_id=block_document_id,
                subject=NOTIFICATION_SUBJECT,
                body=NOTIFICATION_BODY,
            )
        ],
    )

    existing = await client.read_automations_by_name(name=AUTOMATION_NAME)
    if existing:
        auto_id = existing[0].id
        print(f"  Automation already exists ({auto_id}), updating...")
        await client.update_automation(auto_id, automation)
        print("  Automation updated.")
        return auto_id

    print("  Creating automation...")
    auto_id = await client.create_automation(automation)
    print(f"  Automation created ({auto_id}).")
    return auto_id


async def main(webhook_url: str, api_key: str):
    print(f"Backend webhook URL: {webhook_url}")
    print()

    async with get_client() as client:
        print("Step 1: Setting up webhook block document...")
        block_doc_id = await setup_block(webhook_url, api_key)

        print("\nStep 2: Setting up automation...")
        auto_id = await setup_automation(client, block_doc_id)

    print(f"\nDone. Automation '{AUTOMATION_NAME}' is active (id={auto_id}).")
    print(f"Flow run state changes will be POSTed to: {webhook_url}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set up Dalgo webhook automation in Prefect")
    parser.add_argument("--backend-webhook-url", required=True, help="e.g. http://backend:8002/webhooks/v1/notification/")
    parser.add_argument("--backend-api-key", required=True, help="Value of PREFECT_NOTIFICATIONS_WEBHOOK_KEY from DDP_backend/.env")
    args = parser.parse_args()

    asyncio.run(main(args.backend_webhook_url, args.backend_api_key))
