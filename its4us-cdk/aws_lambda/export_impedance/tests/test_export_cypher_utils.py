import pytest

from cypher_utils import identifier


def test_identifier_quotes_allowed_identifier():
    assert identifier("Some-LowVision", {"Some-LowVision"}) == "`Some-LowVision`"


def test_identifier_rejects_query_injection():
    with pytest.raises(ValueError):
        identifier("x` RETURN n //")


def test_identifier_rejects_unapproved_column():
    with pytest.raises(ValueError):
        identifier("OtherProperty", {"Some", "Device"})
