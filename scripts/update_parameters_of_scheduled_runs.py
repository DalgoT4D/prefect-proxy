"""script to set project_dir for a deployment / all deployments"""

import sys
import argparse
import requests

BASE_URL = "http://localhost:4200/api"

parser = argparse.ArgumentParser(usage="updates parameters for scheduled flow-runs")
parser.add_argument("--show", action="store_true")
args = parser.parse_args()

payload = {
    "flow_runs": {
        "operator": "and_",
        "tags": {"operator": "and_", "any_": ["auto-scheduled"]},
        "state": {
            "operator": "and_",
            "type": {
                "any_": ["SCHEDULED"],
            },
        },
    },
    "offset": 0,
    "limit": 50,
}

while True:
    r = requests.post(f"{BASE_URL}/flow_runs/filter", json=payload, timeout=20)
    try:
        r.raise_for_status()
    except Exception:
        print("could not fetch flow_runs using filter payload")
        print(payload)
        sys.exit(1)

    flow_runs = r.json()

    for flow_run in flow_runs:
        flow_run_id = flow_run["id"]

        changed = False
        show = False
        tasks = flow_run.get("parameters", {}).get("config", {}).get("tasks", [])

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
            print(flow_run["id"])
            if len(tasks) > 0:
                print(flow_run)

        elif changed:
            update_payload = {"parameters": flow_run["parameters"]}
            print(update_payload)
            print("=" * 40)
            response = requests.patch(
                f"{BASE_URL}/flow_runs/{flow_run_id}",
                json=update_payload,
                timeout=20,
            )
            try:
                response.raise_for_status()
            except Exception:
                print(response.json())
                break
        else:
            print(f"no change to {flow_run_id}")

    if "offset" in payload and len(flow_runs) > 0:
        payload["offset"] += payload["limit"]
        print(f"offset={payload['offset']}")

    else:
        break
