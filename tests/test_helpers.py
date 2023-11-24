"""tests for proxy.helpers"""
from proxy.helpers import cleaned_name_for_prefectblock, command_from_dbt_blockname


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
