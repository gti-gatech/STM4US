import pytest

from .constants import constants
from preprocess import PreprocessNavigatorData


@pytest.fixture(scope="session")
def scheduled_events_input_data():

    # define scheduled events input data
    file1 = open("its4us-cdk/aws_lambda/import_navigator/tests/data/scheduled_events.txt", "rb")
    raw_text = file1.read()
    file1.close()
    scheduled_events_input_data = raw_text.decode("unicode-escape")

    return scheduled_events_input_data


@pytest.fixture(scope="session")
def scheduled_events_input_pd(constants, scheduled_events_input_data):

    # turn scheduled_events data from raw text to pandas dataframe
    preprocessObj = PreprocessNavigatorData(constants, scheduled_events_input_data, 
                                            None, None, None)
    
    scheduled_events_input_pd = preprocessObj.preprocess_scheduled_events()

    return scheduled_events_input_pd
