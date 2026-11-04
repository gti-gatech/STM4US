import pytest
import pandas as pd


@pytest.fixture(scope="session")
def waze_mul_data():
    data = {
        "stmAdaPathLinkID": [200001, 200001],
        "factor": [1.5, 1.2],
        "type": ["MUL", "MUL"],
    }
    return pd.DataFrame(data)


@pytest.fixture(scope="session")
def waze_add_data():
    data = {
        "stmAdaPathLinkID": [200001, 200001],
        "factor": [5.0, 3.0],
        "type": ["ADD", "ADD"],
    }
    return pd.DataFrame(data)


@pytest.fixture(scope="session")
def waze_combined_data():
    data = {
        "stmAdaPathLinkID": [200001, 200001, 200001],
        "factor": [1.5, 1.2, 5.0],
        "type": ["MUL", "MUL", "ADD"],
    }
    return pd.DataFrame(data)


@pytest.fixture(scope="session")
def empty_waze_data():
    return pd.DataFrame(columns=["stmAdaPathLinkID", "factor", "type"])
