import re


def cleaned_name_for_prefectblock(blockname):
    """removes characters which are not lowercase letters, digits or dashes"""
    pattern = re.compile(r"[^\-a-z0-9]")
    return re.sub(pattern, "", blockname.lower())
