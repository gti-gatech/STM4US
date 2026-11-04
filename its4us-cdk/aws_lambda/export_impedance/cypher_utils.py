import re


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")


def identifier(value, allowed=None):
    if allowed is not None and value not in allowed:
        raise ValueError("Unsupported Cypher identifier")
    if allowed is not None:
        return f"`{value}`"
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError("Invalid Cypher identifier")
    return f"`{value}`"
