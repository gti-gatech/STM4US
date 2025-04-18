import sys
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

# mock libraries not being used in the tests
sys.modules["shapely.geometry"] = MagicMock()
sys.modules["shapely.geometry.polygon"] = MagicMock()
sys.modules["shapely.ops"] = MagicMock()
sys.modules["pyproj"] = MagicMock()
sys.modules["neo4j"] = MagicMock()

from query_writer_navigator import NavigatorEventQueries


@pytest.mark.order(2)
class TestNavigatorIngestions:

    @patch("query_writer_navigator.NavigatorEventQueries.events_in_square_box")
    def test_filter_events(self, query_url, mock_events_in_square_box, scheduled_events_input_pd, 
                           unscheduled_events_input_pd, comments_input_pd,
                           properties_input_pd):

        # define inputs to the NavigatorEventQueries class
        env = "dev"
        method = "POST"
        scheduled_events = scheduled_events_input_pd # fixture
        unscheduled_events = unscheduled_events_input_pd # fixture
        comments = comments_input_pd # fixture
        properties = properties_input_pd # fixture
        sidewalk_records = None # not used
        crosswalk_records = None # not used
        data_set_id = "34.0N84.4W"

        # define the expected outputs of the test function
        expected_scheduled_event_ids = ["3840629", "3840638"]
        expected_scheduled_versions = [2, 2]
        expected_scheduled_numrows = 2
        expected_unscheduled_event_ids = ["3840615", "3840632"]
        expected_unscheduled_versions = [2, 3]
        expected_unscheduled_numrows = 2

        # call the NavigatorEventQueries class with defined inputs
        navigatorObj = NavigatorEventQueries(query_url, method, scheduled_events, unscheduled_events, 
                                             comments, properties, sidewalk_records, crosswalk_records, 
                                             data_set_id)
        
        # run the test function on scheduled and unscheduled events data
        mock_events_in_square_box.side_effect = [scheduled_events, unscheduled_events] # mock the return value of events_in_square_box
        filtered_scheduled_events = navigatorObj.filter_events(scheduled_events)
        filtered_unscheduled_events = navigatorObj.filter_events(unscheduled_events)

        # ensure the outputs data are dataframe
        assert isinstance(filtered_scheduled_events, pd.DataFrame)
        assert isinstance(filtered_unscheduled_events, pd.DataFrame)

        # ensure the expected event ids exist in the data
        for expected_scheduled_event_id in expected_scheduled_event_ids:
            assert expected_scheduled_event_id in filtered_scheduled_events["event_id"].values
        for expected_unscheduled_event_id in expected_unscheduled_event_ids:
            assert expected_unscheduled_event_id in filtered_unscheduled_events["event_id"].values
        
        # ensure other fields such as version have correct values
        count = 0
        for expected_scheduled_event_id in expected_scheduled_event_ids:
            version = filtered_scheduled_events.loc[filtered_scheduled_events["event_id"] 
                                                    == expected_scheduled_event_id, "version"].iloc[0]

            assert version == expected_scheduled_versions[count]

            count += 1

        count = 0
        for expected_unscheduled_event_id in expected_unscheduled_event_ids:
            version = filtered_unscheduled_events.loc[filtered_unscheduled_events["event_id"] 
                                                      == expected_unscheduled_event_id, "version"].iloc[0]

            assert version == expected_unscheduled_versions[count]

            count += 1

        # ensure number of rows after filtering are correct
        assert filtered_scheduled_events.shape[0] == expected_scheduled_numrows
        assert filtered_unscheduled_events.shape[0] == expected_unscheduled_numrows


    def test_filter_comments(self, query_url, scheduled_events_input_pd, unscheduled_events_input_pd, 
                             comments_input_pd, properties_input_pd):

        # define inputs to the NavigatorEventQueries class
        env = "dev"
        method = "POST"
        scheduled_events = scheduled_events_input_pd # fixture
        unscheduled_events = unscheduled_events_input_pd # fixture
        comments = comments_input_pd # fixture
        properties = properties_input_pd # fixture
        sidewalk_records = None # not used
        crosswalk_records = None # not used
        data_set_id = "34.0N84.4W"

        # call the NavigatorEventQueries class with defined inputs
        navigatorObj = NavigatorEventQueries(query_url, method, scheduled_events, unscheduled_events, 
                                             comments, properties, sidewalk_records, crosswalk_records, 
                                             data_set_id)
        
        # run the test function on comments data
        event_ids = ["3840629", "3840638"]
        for event_id in event_ids:

            filtered_comments = navigatorObj.filter_comments(event_id)

            # ensure the filtered comments have expected outputs
            assert isinstance(filtered_comments, pd.DataFrame)

            if event_id == "3840629":

                # ensure the comment id is 11148108
                comment_id = filtered_comments.loc[filtered_comments["event_id"] == event_id, "comment_id"].iloc[0]

                assert comment_id == "11148108"
                assert filtered_comments.shape[0] == 1
            
            else:

                # ensure the comment id contains 11148117 and 11148213
                expected_comment_ids = ["11148117", "11148213"]
                for expected_comment_id in expected_comment_ids:
                    assert expected_comment_id in filtered_comments["comment_id"].values
                
                assert filtered_comments.shape[0] == 2


    def test_filter_properties(self, query_url, scheduled_events_input_pd, unscheduled_events_input_pd, 
                               comments_input_pd, properties_input_pd):
        
        # define inputs to the NavigatorEventQueries class
        env = "dev"
        method = "POST"
        scheduled_events = scheduled_events_input_pd # fixture
        unscheduled_events = unscheduled_events_input_pd # fixture
        comments = comments_input_pd # fixture
        properties = properties_input_pd # fixture
        sidewalk_records = None # not used
        crosswalk_records = None # not used
        data_set_id = "34.0N84.4W"

        # call the NavigatorEventQueries class with defined inputs
        navigatorObj = NavigatorEventQueries(query_url, method, scheduled_events, unscheduled_events, 
                                             comments, properties, sidewalk_records, crosswalk_records, 
                                             data_set_id)

        # define the expected outputs of the test function
        expected_property_ids = ["57118690", "57118691", "57118750", "57118751"]
        expected_numrows = 16

        # run the test function on properties data
        event_id = "3840629"
        filtered_properties = navigatorObj.filter_properties(event_id)

        # ensure the output data is a dataframe
        assert isinstance(filtered_properties, pd.DataFrame)

        # ensure the expected property ids exist in the data
        for expected_property_id in expected_property_ids:
            assert expected_property_id in filtered_properties["property_id"].values

        # ensure number of rows after filtering is correct
        assert filtered_properties.shape[0] == expected_numrows


    @patch("query_writer_navigator.NavigatorEventQueries.events_in_square_box")
    def test_find_event_impedance(self, query_url, mock_events_in_square_box, scheduled_events_input_pd, 
                                  unscheduled_events_input_pd, comments_input_pd, 
                                  properties_input_pd):
        
        # define inputs to the NavigatorEventQueries class
        env = "dev"
        method = "POST"
        scheduled_events = scheduled_events_input_pd # fixture
        unscheduled_events = unscheduled_events_input_pd # fixture
        comments = comments_input_pd # fixture
        properties = properties_input_pd # fixture
        sidewalk_records = None # not used
        crosswalk_records = None # not used
        data_set_id = "34.0N84.4W"

        # call the NavigatorEventQueries class with defined inputs
        navigatorObj = NavigatorEventQueries(query_url, method, scheduled_events, unscheduled_events, 
                                             comments, properties, sidewalk_records, crosswalk_records, 
                                             data_set_id)
        
        # define the expected outputs of the test function
        expected_scheduled_factors = [2, 2]
        expected_scheduled_effect_types = ["MUL", "MUL"]
        expected_unscheduled_factors = [2, 2]
        expected_unscheduled_effect_types = ["MUL", "MUL"]

        # filter events data before running the test function
        mock_events_in_square_box.side_effect = [scheduled_events, unscheduled_events] # mock the return value of events_in_square_box
        filtered_scheduled_events = navigatorObj.filter_events(scheduled_events)
        filtered_unscheduled_events = navigatorObj.filter_events(unscheduled_events)

        # run the test function on scheduled events
        scheduled_factors = []
        scheduled_effect_types = []
        for _, event in filtered_scheduled_events.iterrows():

            factor, effect_type = navigatorObj.find_event_impedance(event, "SCHEDULED", "")
            scheduled_factors.append(factor)
            scheduled_effect_types.append(effect_type)

        assert scheduled_factors == expected_scheduled_factors
        assert scheduled_effect_types == expected_scheduled_effect_types

        # run the test function on unscheduled events
        unscheduled_factors = []
        unscheduled_effect_types = []
        for _, event in filtered_unscheduled_events.iterrows():

            factor, effect_type = navigatorObj.find_event_impedance(event, "UNSCHEDULED", "")
            unscheduled_factors.append(factor)
            unscheduled_effect_types.append(effect_type)

        assert unscheduled_factors == expected_unscheduled_factors
        assert unscheduled_effect_types == expected_unscheduled_effect_types
