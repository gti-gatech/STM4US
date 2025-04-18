import pandas as pd
import pytest
from unittest.mock import patch

from preprocess import PreprocessNavigatorData


@pytest.mark.order(1)
class TestNavigatorPreprocess:

    @patch("preprocess.PreprocessNavigatorData.add_modified_date_ms")
    def test_preprocess_scheduled_events(self, mock_add_modified_date_ms, scheduled_events_input_data, 
                                         unscheduled_events_input_data, comments_input_data, 
                                         properties_input_data):
        
        # mock the return value of add_modified_date_ms
        mock_add_modified_date_ms.return_value = 0.0
        
        # define inputs to the PreprocessNavigatorData class
        scheduled_events = scheduled_events_input_data # fixture
        unscheduled_events = unscheduled_events_input_data # fixture
        comments = comments_input_data # fixture
        properties = properties_input_data # fixture

        # define the expected outputs of the test function
        expected_event_ids = ["3840629", "3840638"]
        expected_types = ["Planned", "Planned"]
        expected_subtypes = ["Construction", "Maintenance Activity"]

        # call the PreprocessNavigatorData class with defined inputs
        preprocessObj = PreprocessNavigatorData(scheduled_events, unscheduled_events,
                                                comments, properties)
        
        # run the test function
        df = preprocessObj.preprocess_scheduled_events()

        # ensure the output df is a dataframe
        assert isinstance(df, pd.DataFrame)

        # ensure the expected event ids exist in df
        for expected_event_id in expected_event_ids:
            assert expected_event_id in df["event_id"].values

        # ensure other fields such as types and subtype have correct values
        count = 0
        for expected_event_id in expected_event_ids:
            event_type = df.loc[df["event_id"] == expected_event_id, "type"].iloc[0]
            subtype = df.loc[df["event_id"] == expected_event_id, "subtype"].iloc[0]

            assert event_type == expected_types[count]
            assert subtype == expected_subtypes[count]

            count += 1

        # ensure the data types are expected in df
        assert df["event_id"].dtypes.name == "string"
        assert df["version"].dtypes.name == "int64"
        assert df["latitude"].dtypes.name == "float64"
        assert df["longitude"].dtypes.name == "float64"

    
    @patch("preprocess.PreprocessNavigatorData.add_modified_date_ms")
    def test_preprocess_unscheduled_events(self, mock_add_modified_date_ms, scheduled_events_input_data, 
                                           unscheduled_events_input_data, comments_input_data, 
                                           properties_input_data):
        
        # mock the return value of add_modified_date_ms
        mock_add_modified_date_ms.return_value = 0.0
        
        # define inputs to the PreprocessNavigatorData class
        scheduled_events = scheduled_events_input_data # fixture
        unscheduled_events = unscheduled_events_input_data # fixture
        comments = comments_input_data # fixture
        properties = properties_input_data # fixture

        # define the expected outputs of the test function
        expected_event_ids = ["3840615", "3840632"]
        expected_types = ["Debris", "Debris"]
        expected_subtypes = ["Other", "Other"]

        # call the PreprocessNavigatorData class with defined inputs
        preprocessObj = PreprocessNavigatorData(scheduled_events, unscheduled_events,
                                                comments, properties)
        
        # run the test function
        df = preprocessObj.preprocess_unscheduled_events()

        # ensure the output df is a dataframe
        assert isinstance(df, pd.DataFrame)

        # ensure the expected event ids exist in df
        for expected_event_id in expected_event_ids:
            assert expected_event_id in df["event_id"].values

        # ensure other fields such as types and subtype have correct values
        count = 0
        for expected_event_id in expected_event_ids:
            event_type = df.loc[df["event_id"] == expected_event_id, "type"].iloc[0]
            subtype = df.loc[df["event_id"] == expected_event_id, "subtype"].iloc[0]

            assert event_type == expected_types[count]
            assert subtype == expected_subtypes[count]
            
            count += 1

        # ensure the data types are expected in df
        assert df["event_id"].dtypes.name == "string"
        assert df["version"].dtypes.name == "int64"
        assert df["latitude"].dtypes.name == "float64"
        assert df["longitude"].dtypes.name == "float64"

    
    def test_preprocess_comments(self, scheduled_events_input_data, unscheduled_events_input_data, 
                                 comments_input_data, properties_input_data):
        
        # define inputs to the PreprocessNavigatorData class
        scheduled_events = scheduled_events_input_data # fixture
        unscheduled_events = unscheduled_events_input_data # fixture
        comments = comments_input_data # fixture
        properties = properties_input_data # fixture

        # define the expected outputs of the test function
        expected_comment_ids = ["11148108", "11148117", "11148213"]
        expected_event_ids = ["3840629", "3840638", "3840638"]

        # call the PreprocessNavigatorData class with defined inputs
        preprocessObj = PreprocessNavigatorData(scheduled_events, unscheduled_events,
                                                comments, properties)
        
        # run the test function
        df = preprocessObj.preprocess_comments()

        # ensure the output df is a dataframe
        assert isinstance(df, pd.DataFrame)

        # ensure the expected comment ids exist in df
        for expected_comment_id in expected_comment_ids:
            assert expected_comment_id in df["comment_id"].values

        # ensure other fields such as event_id have correct values
        count = 0
        for expected_comment_id in expected_comment_ids:
            event_id = df.loc[df["comment_id"] == expected_comment_id, "event_id"].iloc[0]

            assert event_id == expected_event_ids[count]
            
            count += 1

        # ensure the data types are expected in df
        assert df["comment_id"].dtypes.name == "string"
        assert df["event_id"].dtypes.name == "string"
        assert df["agency"].dtypes.name == "string"
        assert df["comments"].dtypes.name == "string"


    def test_preprocess_properties(self, scheduled_events_input_data, unscheduled_events_input_data, 
                                   comments_input_data, properties_input_data):
        
        # define inputs to the PreprocessNavigatorData class
        scheduled_events = scheduled_events_input_data # fixture
        unscheduled_events = unscheduled_events_input_data # fixture
        comments = comments_input_data # fixture
        properties = properties_input_data # fixture

        # define the expected outputs of the test function
        expected_property_ids = ["57118690", "57118691", "57118692"]
        expected_event_ids = ["3840629", "3840629", "3840629"]

        # call the PreprocessNavigatorData class with defined inputs
        preprocessObj = PreprocessNavigatorData(scheduled_events, unscheduled_events,
                                                comments, properties)
        
        # run the test function
        df = preprocessObj.preprocess_properties()

        # ensure the output df is a dataframe
        assert isinstance(df, pd.DataFrame)

        # ensure the expected property ids exist in df
        for expected_property_id in expected_property_ids:
            assert expected_property_id in df["property_id"].values

        # ensure other fields such as event_id have correct values
        count = 0
        for expected_property_id in expected_property_ids:
            event_id = df.loc[df["property_id"] == expected_property_id, "event_id"].iloc[0]

            assert event_id == expected_event_ids[count]
            
            count += 1

        # ensure the data types are expected in df
        assert df["property_id"].dtypes.name == "string"
        assert df["event_id"].dtypes.name == "string"
        assert df["version"].dtypes.name == "int64"
        assert df["type"].dtypes.name == "string"