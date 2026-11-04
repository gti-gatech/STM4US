import json
import pytest


@pytest.fixture(scope="session")
def waze_input_data():

    # define waze input data
    DATA_PATH = "its4us-cdk/aws_lambda/import_waze/tests/data/waze.json"
    fpath = open(DATA_PATH)
    waze_input_data = json.load(fpath)

    return waze_input_data
