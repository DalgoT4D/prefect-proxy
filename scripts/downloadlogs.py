"""script to download the logs for a flow run"""

import json
import argparse
import requests

parser = argparse.ArgumentParser(
    usage="downloads logs for a flow run and prints them to stdout"
)
parser.add_argument("--flow_run_id", required=True)
parser.add_argument("--json", action="store_true")
args = parser.parse_args()

offset: int = 0
entries = []

while True:
    payload = {
        "offset": offset,
        "logs": {
            "operator": "and_",
            "flow_run_id": {"any_": [args.flow_run_id]},
        },
        "sort": "TIMESTAMP_ASC",
    }
    r = requests.post("http://localhost:4200/api/logs/filter", json=payload, timeout=15)
    logs = r.json()
    if len(logs) == 0:
        break
    entries += logs
    offset += len(logs)

if args.json:
    print(json.dumps(entries, indent=2))
else:
    for entry in entries:
        print(f"{entry['timestamp']}  {entry['message']}\n")
