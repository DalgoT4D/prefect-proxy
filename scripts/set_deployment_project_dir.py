"""script to set project_dir for a deployment / all deployments"""

import sys
import argparse
import requests

BASE_URL = "http://localhost:4200/api"

parser = argparse.ArgumentParser(usage="sets project_dir for deployments")
parser.add_argument("--show", action="store_true")
parser.add_argument("--deployment-id")
args = parser.parse_args()

if args.deployment_id:
    payload = {
        "deployments": {"operator": "and_", "id": {"any_": [args.deployment_id]}},
    }
else:
    payload = {"offset": 0, "limit": 50}

while True:
    r = requests.post(f"{BASE_URL}/deployments/filter", json=payload, timeout=20)
    try:
        r.raise_for_status()
    except Exception:
        print("could not fetch deployments using filter payload")
        print(payload)
        sys.exit(1)

    deployments = r.json()

    for deployment in deployments:
        deployment_id = deployment["id"]

        changed = False
        show = False
        tasks = deployment.get("parameters", {}).get("config", {}).get("tasks", [])

        for task in tasks:
            if show:
                break
            for field in ["project_dir", "working_dir", "profiles_dir"]:
                if task.get(field) and "/home/ddp/clientdbts" in task.get(field):
                    if args.show:
                        show = True
                        break
                    task[field] = task[field].replace(
                        "/home/ddp/clientdbts", "/mnt/appdata/clientdbts"
                    )
                    changed = True

        if show:
            print(deployment["id"])

        elif changed:
            update_payload = {"parameters": deployment["parameters"]}
            print(update_payload)
            print("=" * 40)
            response = requests.patch(
                f"{BASE_URL}/deployments/{deployment_id}",
                json=update_payload,
                timeout=20,
            )
            try:
                response.raise_for_status()
            except Exception:
                print(response.json())
                break
        else:
            print(f"no change to {deployment_id}")

    if "offset" in payload and len(deployments) > 0:
        payload["offset"] += payload["limit"]
        print(f"offset={payload['offset']}")
    else:
        break
