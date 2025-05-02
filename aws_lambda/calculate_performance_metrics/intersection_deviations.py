import sys
sys.path.append("/mnt/fs1") # import shapely library from AWS EFS

from shapely.geometry import Point, LineString
from shapely.geometry.polygon import Polygon
from shapely.ops import nearest_points
from pyproj import Geod

from graph_database_driver import GraphDatabaseDriver


class IntersectionDeviations:

    def __init__(self, env: str, deviated_coords: Point, query_url: str) -> None:

        self.env = env
        self.query_url = query_url
        self.deviated_coords = deviated_coords

        # set WGS84 geospatial CRS
        self.wgs84_geod = Geod(ellps = "WGS84")

        self.meter_to_feet = 3.28084 # 1 meter is 3.28084 feet
        self.buffer_length = 20.0 # ft

        self.study_area = {
            "34.0N84.4W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.4, "max_lon": -84.3},
            "33.8N84.4W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.4, "max_lon": -84.3},
            "33.9N84.4W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.4, "max_lon": -84.3},
            "33.8N84.1W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.1, "max_lon": -84.0},
            "34.0N84.3W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.3, "max_lon": -84.2},
            "33.8N84.3W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.3, "max_lon": -84.2},
            "33.9N84.3W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.3, "max_lon": -84.2},
            "33.8N84.2W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.2, "max_lon": -84.1},
            "33.9N84.2W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.2, "max_lon": -84.1},
            "34.0N84.2W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.2, "max_lon": -84.1},
            "33.9N84.1W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.1, "max_lon": -84.0},
            "34.0N84.1W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.1, "max_lon": -84.0},
            "33.9N84.0W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.0, "max_lon": -83.9},
            "34.0N84.0W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.0, "max_lon": -83.9},
        }


    def find_study_area_cell(self) -> str:

        datasetid = ""

        # set up the square to check if the deviated_coords point is in the square
        for key, cell in self.study_area.items():

            grid_points = [(cell["min_lat"], cell["min_lon"]), (cell["max_lat"], cell["min_lon"]),
                           (cell["max_lat"], cell["max_lon"]), (cell["min_lat"], cell["max_lon"])]

            square = Polygon(grid_points)
            inbox = square.contains(self.deviated_coords)

            if inbox:

                datasetid = key
                break

        if not datasetid:

            # a grid cell of interest is not found
            message = "WARNING: Unable to find a grid cell from the study area that contains the point: " \
                + str(self.deviated_coords) + ". An arbitrary grid cell will be used."
            print(message)

            datasetid = "34.0N84.4W" # arbitrary grid cell
        
        return datasetid


    def retrieve_crosswalk_nodes(self, datasetid: str) -> list:

        # retrieve OSM crosswalk nodes that represent traffic light intersections within a grid cell
        crosswalk_query1 = "MATCH (node1:`OSM-NODE`)-[:FIRST]-(crosswalk:`OSM-WAY` "
        crosswalk_query2 = "{{footway: 'crossing', crossing: 'traffic_signals', __datasetid: '{}'}})-"
        crosswalk_query3 = "[:LAST]-(node2:`OSM-NODE`) RETURN crosswalk, node1, node2"
        crosswalk_query = crosswalk_query1 + crosswalk_query2 + crosswalk_query3
        crosswalk_query = crosswalk_query.format(datasetid)
        
        driverObj = GraphDatabaseDriver(self.env, self.query_url)
        crosswalk_records = driverObj.run_query("CHECK", crosswalk_query)

        return crosswalk_records


    def sort_crosswalk_nodes(self, crosswalk_records: list) -> tuple[list, list, list]:

        # order crosswalk nodes that represent traffic light intersections w.r.t. the deviated point
        crosswalk_nodes = []
        crosswalk_distances = []

        first_crosswalk_latlon = []
        last_crosswalk_latlon = []

        for crosswalk_record in crosswalk_records:

            crosswalk = crosswalk_record.data()["crosswalk"]

            # find start and end points in lat/lon of the crosswalk
            start_node = crosswalk_record.data()["node1"]
            start_node_lat = start_node["lat"]
            start_node_lon = start_node["lon"]

            end_node = crosswalk_record.data()["node2"]
            end_node_lat = end_node["lat"]
            end_node_lon = end_node["lon"]

            # create the crosswalk line
            crosswalk_line = LineString(((start_node_lat, start_node_lon), (end_node_lat, end_node_lon)))

            # find the nearest point on the crosswalk line to the deviated point
            near_points = nearest_points(crosswalk_line, self.deviated_coords)

            # determine the distance in meters between the two points
            lat1 = near_points[0].x # y if use (lon, lat) as inputs to Point and LineString above
            lon1 = near_points[0].y # x if use (lon, lat) as inputs to Point and LineString above
            lat2 = near_points[1].x # y if use (lon, lat) as inputs to Point and LineString above
            lon2 = near_points[1].y # x if use (lon, lat) as inputs to Point and LineString above
            _, _, dist = self.wgs84_geod.inv(lon1, lat1, lon2, lat2)

            # convert the distance from meters to feet
            distance = self.meter_to_feet * dist

            # store the crosswalk node and the computed distance
            crosswalk_nodes.append(crosswalk)
            crosswalk_distances.append(distance)

            # store the lat/lon of start and end points of the crosswalk node
            first_crosswalk_latlon.append((start_node_lat, start_node_lon))
            last_crosswalk_latlon.append((end_node_lat, end_node_lon))
        
        # order the crosswalk nodes found based on the computed distances
        sorted_crosswalk_nodes = [val for (_, val) in sorted(zip(crosswalk_distances, crosswalk_nodes), key=lambda x: x[0])]

        # order the first and last points of the crosswalk nodes based on the computed distances
        sorted_first_crosswalk_latlon = [val for (_, val) in sorted(zip(crosswalk_distances, first_crosswalk_latlon), key=lambda x: x[0])]
        sorted_last_crosswalk_latlon = [val for (_, val) in sorted(zip(crosswalk_distances, last_crosswalk_latlon), key=lambda x: x[0])]

        return sorted_crosswalk_nodes, sorted_first_crosswalk_latlon, sorted_last_crosswalk_latlon


    def find_square_box_points(self, buffered_distance: float, close_first_crosswalk_latlon: tuple, 
                               close_last_crosswalk_latlon: tuple, far_first_crosswalk_latlon: tuple, 
                               far_last_crosswalk_latlon: tuple) -> Polygon:

            # form the square based on the 4 points found
            points = [close_first_crosswalk_latlon, close_last_crosswalk_latlon,
                      far_first_crosswalk_latlon, far_last_crosswalk_latlon]
            square = Polygon(points)

            # find centroid of the square
            centroid = square.centroid # Point

            # draw intersection square with the buffered distance
            buffered_square = centroid.buffer(buffered_distance, cap_style = 3)

            return buffered_square


    def check_deviated_intersection(self, sorted_crosswalk_nodes: list, sorted_first_crosswalk_latlon: list, 
                                    sorted_last_crosswalk_latlon: list) -> bool:

        # check to see if the deviated point is inside a buffered square box
        inbox = False

        if len(sorted_crosswalk_nodes) >= 4: # sufficient to check 4 closest crosswalk nodes

            # find the length of the nearest crosswalk
            lat1 = sorted_first_crosswalk_latlon[0][0]
            lon1 = sorted_first_crosswalk_latlon[0][1]
            lat2 = sorted_last_crosswalk_latlon[0][0]
            lon2 = sorted_last_crosswalk_latlon[0][1]
            _, _, dist = self.wgs84_geod.inv(lon1, lat1, lon2, lat2)

            # convert the distance from meters to feet and divide by 2 to get half of the square length plus buffer
            buffered_distance = self.meter_to_feet * dist / 2.0 + self.buffer_length

            # draw intersection square with the buffered_distance
            close_first_crosswalk_latlon = sorted_first_crosswalk_latlon[0]
            close_last_crosswalk_latlon = sorted_last_crosswalk_latlon[0]
            far_first_crosswalk_latlon = sorted_first_crosswalk_latlon[3]
            far_last_crosswalk_latlon = sorted_last_crosswalk_latlon[3]
            buffered_square = self.find_square_box_points(buffered_distance, close_first_crosswalk_latlon, 
                                                          close_last_crosswalk_latlon, far_first_crosswalk_latlon, 
                                                          far_last_crosswalk_latlon)
            
            # check to see if the deviated point is in the square box
            inbox = buffered_square.contains(self.deviated_coords)
        
        return inbox
    

    @classmethod
    def find_intersection_deviations_class(cls, env: str, lat: float, lon: float, query_url: str) -> bool:

        # form the deviated point
        deviated_coords = Point(lat, lon)

        # initialize the class object
        deviatedObj = cls(env, deviated_coords, query_url)

        # retrieve OSM-WAY crosswalk nodes that represent traffic light intersections
        datasetid = deviatedObj.find_study_area_cell()
        crosswalk_records = deviatedObj.retrieve_crosswalk_nodes(datasetid)

        # sort crosswalk traffic light intersections based on the deviated point of interest
        sorted_crosswalk_nodes, sorted_first_crosswalk_latlon, sorted_last_crosswalk_latlon \
        = deviatedObj.sort_crosswalk_nodes(crosswalk_records)

        # determine if the deviation occurs near a traffic light intersection
        inbox = deviatedObj.check_deviated_intersection(sorted_crosswalk_nodes, sorted_first_crosswalk_latlon, 
                                                        sorted_last_crosswalk_latlon)


        return inbox