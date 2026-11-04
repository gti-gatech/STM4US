import pytest

from cypher_utils import identifier


def test_identifier_rejects_query_injection():
    with pytest.raises(ValueError):
        identifier("dataset` MATCH (n) RETURN n //")


def test_identifier_accepts_known_travel_type():
    assert identifier("Some-LowVision", {"Some-LowVision"}) == "`Some-LowVision`"
