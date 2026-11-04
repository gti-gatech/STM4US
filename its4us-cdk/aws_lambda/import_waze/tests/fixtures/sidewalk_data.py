import json
import pytest


@pytest.fixture(scope="session")
def sidewalk_input_data():

    DATA_PATH = "its4us-cdk/aws_lambda/import_waze/tests/data/sidewalk.json"
    fpath = open(DATA_PATH)
    sidewalk_data = json.load(fpath)

    # define sidewalk input data
    sidewalk_input_data = []

    for sidewalk in sidewalk_data:

        sidewalk_input_data.append(sidewalk["sidewalk"])

    return sidewalk_input_data