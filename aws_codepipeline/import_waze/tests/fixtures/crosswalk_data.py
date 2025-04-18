import json
import pytest


@pytest.fixture(scope="session")
def crosswalk_input_data():

    DATA_PATH = "tests/data/crosswalk.json"
    fpath = open(DATA_PATH)
    crosswalk_data = json.load(fpath)

    # define crosswalk input data
    crosswalk_input_data = []

    for crosswalk in crosswalk_data:

        crosswalk_input_data.append(crosswalk["crosswalk"])

    return crosswalk_input_data