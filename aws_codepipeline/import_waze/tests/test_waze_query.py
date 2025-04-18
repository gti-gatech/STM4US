import sys
from unittest.mock import MagicMock, patch

# mock libraries not being used in the tests
sys.modules["shapely.geometry"] = MagicMock()
sys.modules["shapely.geometry.polygon"] = MagicMock()
sys.modules["shapely.ops"] = MagicMock()
sys.modules["pyproj"] = MagicMock()
sys.modules["neo4j"] = MagicMock()

from query_writer_waze import WazeAlertsQueries


class TestWazeIngestions:

    def test_sort_sidewalks_crosswalks(self, query_url, waze_input_data, sidewalk_input_data,
                                       crosswalk_input_data):

        # define inputs to the WazeAlertsQueries class
        env = "dev"
        method = "POST"
        data = waze_input_data # fixture
        sidewalk_records = sidewalk_input_data # fixture
        crosswalk_records = crosswalk_input_data # fixture

        # define the expected outputs of the test function 
        expected_sorted_sidewalk_node_ids = ["544894254", "544894255", "544894262"]
        expected_sorted_sidewalk_distances = [31110.53, 31154.23, 31663.34] # round to two decimal places
        expected_sorted_crosswalk_node_ids = ["1111062736", "1111062737", "1111062735"]
        expected_sorted_crosswalk_distances = [30603.45, 30606.44, 31395.31] # round to two decimal places
        expected_sorted_first_crosswalk_latlon = [(33.89015, -84.14857), (33.89015, -84.14854), (33.892887, -84.14973)]
        expected_sorted_last_crosswalk_latlon = [(33.89015, -84.14854), (33.890167, -84.148476), (33.89279, -84.14981)]

        # call the WazeAlertsQueries class with defined inputs
        wazeQueryObj = WazeAlertsQueries(query_url, method, data, sidewalk_records, crosswalk_records)

        # specify the test inputs
        sidewalk_distances = [31663.34, 31154.23, 31110.53]
        crosswalk_distances = [31395.31, 30603.45, 30606.44]
        first_crosswalk_latlon = [(33.892887, -84.14973), (33.89015, -84.14857), (33.89015, -84.14854)]
        last_crosswalk_latlon = [(33.89279, -84.14981), (33.89015, -84.14854), (33.890167, -84.148476)]

        # run the test function
        sorted_sidewalk_nodes, sorted_sidewalk_distances, sorted_crosswalk_nodes, sorted_crosswalk_distances, \
        sorted_first_crosswalk_latlon, sorted_last_crosswalk_latlon \
            = wazeQueryObj.sort_sidewalks_crosswalks(sidewalk_records, sidewalk_distances, 
                                                     crosswalk_records, crosswalk_distances,
                                                     first_crosswalk_latlon, last_crosswalk_latlon)

        sorted_sidewalk_node_ids = []
        sorted_crosswalk_node_ids = []
        
        # extract node ids from the sorted sidewalk and crosswalk nodes
        for node in sorted_sidewalk_nodes:
            sorted_sidewalk_node_ids.append(node["id"])
        
        for node in sorted_crosswalk_nodes:
            sorted_crosswalk_node_ids.append(node["id"])
        
        # make sure the ordered lists are the same between expected and the actual
        assert sorted_sidewalk_node_ids == expected_sorted_sidewalk_node_ids
        assert sorted_crosswalk_node_ids == expected_sorted_crosswalk_node_ids
        assert sorted_sidewalk_distances == expected_sorted_sidewalk_distances
        assert sorted_crosswalk_distances == expected_sorted_crosswalk_distances
        assert sorted_first_crosswalk_latlon == expected_sorted_first_crosswalk_latlon
        assert sorted_last_crosswalk_latlon == expected_sorted_last_crosswalk_latlon


    def test_find_waze_impedance(self, query_url, waze_input_data, sidewalk_input_data,
                                 crosswalk_input_data):

        # define inputs to the WazeAlertsQueries class
        env = "dev"
        method = "POST"
        data = waze_input_data # fixture
        sidewalk_records = sidewalk_input_data # fixture
        crosswalk_records = crosswalk_input_data # fixture

        # define expected outputs of the test function
        expected_factors = [0, 0, 10]
        expected_effect_types = ["", "", "MUL"]
    
        alerts = data["alerts"]
        node_label = "SIDEWALK"

        # call the WazeAlertsQueries class with defined inputs
        wazeQueryObj = WazeAlertsQueries(query_url, method, data, sidewalk_records, crosswalk_records)

        count = 0
        for alert in alerts:

            # run the test function
            wazetype = alert["type"]
            subtype = alert["subtype"]
            factor, effect_type = wazeQueryObj.find_waze_impedance(node_label, wazetype, subtype)

            # make sure the values are the same between expected and the actual
            assert factor == expected_factors[count]
            assert effect_type == expected_effect_types[count]

            count += 1

