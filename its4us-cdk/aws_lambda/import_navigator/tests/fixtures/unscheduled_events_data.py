import pytest

from .constants import constants
from preprocess import PreprocessNavigatorData


@pytest.fixture(scope="session")
def unscheduled_events_input_data():

    # define unscheduled events input data
    file1 = open("its4us-cdk/aws_lambda/import_navigator/tests/data/unscheduled_events.txt", "rb")
    raw_text = file1.read()
    file1.close()
    unscheduled_events_input_data = raw_text.decode("unicode-escape")

    return unscheduled_events_input_data


@pytest.fixture(scope="session")
def unscheduled_events_input_pd(constants, unscheduled_events_input_data):

    # turn unscheduled_events data from raw text to pandas dataframe
    preprocessObj = PreprocessNavigatorData(constants, None, unscheduled_events_input_data,
                                            None, None)
    
    unscheduled_events_input_pd = preprocessObj.preprocess_unscheduled_events()

    return unscheduled_events_input_pd
