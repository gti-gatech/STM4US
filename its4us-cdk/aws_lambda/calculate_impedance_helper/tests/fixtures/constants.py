import pytest


@pytest.fixture(scope="session")
def constants():

    NUM_TRAVEL_TYPES = -18
    MUL = "MUL"
    ADD = "ADD"

    WAZE_FIELDS = {
        "factor": "factor",
        "type": "type",
        "stmAdaPathLinkID": "stmAdaPathLinkID",
    }

    CONSTANTS = {
        "NUM_TRAVEL_TYPES": NUM_TRAVEL_TYPES,
        "MUL": MUL,
        "ADD": ADD,
        "WAZE_FIELDS": WAZE_FIELDS,
    }

    return CONSTANTS
