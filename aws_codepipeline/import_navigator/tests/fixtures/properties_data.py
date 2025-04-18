import pytest

from preprocess import PreprocessNavigatorData


@pytest.fixture(scope="session")
def properties_input_data():

    # define properties input data
    file1 = open("tests/data/properties.txt", "rb")
    raw_text = file1.read()
    file1.close()
    properties_input_data = raw_text.decode("unicode-escape")

    return properties_input_data


@pytest.fixture(scope="session")
def properties_input_pd(properties_input_data):

    # turn comments data from raw text to pandas dataframe
    preprocessObj = PreprocessNavigatorData(None, None, None, properties_input_data)
    
    properties_input_pd = preprocessObj.preprocess_properties()

    return properties_input_pd