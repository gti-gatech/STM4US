import pytest

from preprocess import PreprocessNavigatorData


@pytest.fixture(scope="session")
def comments_input_data():

    # define comments input data
    file1 = open("tests/data/comments.txt", "rb")
    raw_text = file1.read()
    file1.close()
    comments_input_data = raw_text.decode("unicode-escape")

    return comments_input_data


@pytest.fixture(scope="session")
def comments_input_pd(comments_input_data):

    # turn comments data from raw text to pandas dataframe
    preprocessObj = PreprocessNavigatorData(None, None, comments_input_data, None)
    
    comments_input_pd = preprocessObj.preprocess_comments()

    return comments_input_pd