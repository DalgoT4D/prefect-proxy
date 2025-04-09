"""tests for proxy.helpers"""

from proxy.helpers import (
    cleaned_name_for_prefectblock,
    command_from_dbt_blockname,
    deployment_to_json,
)


def test_cleaned_name_for_prefectblock():
    """test for cleaned_name_for_prefectblock"""
    assert cleaned_name_for_prefectblock("block") == "block"
    assert cleaned_name_for_prefectblock("BLOCK") == "block"
    assert cleaned_name_for_prefectblock("block-9") == "block-9"
    assert cleaned_name_for_prefectblock("block+9") == "block9"
    assert cleaned_name_for_prefectblock("\\block+9") == "block9"


def test_command_from_dbt_blockname():
    """tests command_from_dbt_blockname"""
    r = command_from_dbt_blockname("org-profile-target-command")
    assert r == "command"


def test_deployment_to_json_1():
    """tests deployment_to_json"""
    r = deployment_to_json(
        {
            "name": "THE_NAME",
            "id": "THE_ID",
            "tags": ["tag1", "tag2"],
            "schedules": [
                {
                    "schedule": {
                        "cron": "0 0 * * *",
                    },
                    "active": True,
                }
            ],
            "parameters": {
                "key1": "value1",
                "key2": "value2",
            },
        }
    )
    assert r == {
        "name": "THE_NAME",
        "deploymentId": "THE_ID",
        "tags": ["tag1", "tag2"],
        "cron": "0 0 * * *",
        "isScheduleActive": True,
        "parameters": {
            "key1": "value1",
            "key2": "value2",
        },
    }


def test_deployment_to_json_2():
    """tests deployment_to_json"""
    r = deployment_to_json(
        {
            "name": "THE_NAME",
            "id": "THE_ID",
            "tags": ["tag1", "tag2"],
            "parameters": {
                "key1": "value1",
                "key2": "value2",
            },
        }
    )
    assert r == {
        "name": "THE_NAME",
        "deploymentId": "THE_ID",
        "tags": ["tag1", "tag2"],
        "cron": "",
        "isScheduleActive": False,
        "parameters": {
            "key1": "value1",
            "key2": "value2",
        },
    }
