"""
The script consists of openCypher queries for creating/deleting nodes and links for 
Waze alerts data and ingest them to an AWS Neptune database:
    "bolt://<neptune-endpoint>:8182".

UPDATED ON 01/04/24: Waze alerts are attaching to OSM-WAY nodes instead of sidewalksim links nodes.
The OSM-WAY nodes that are attached contain either sidewalk or crossing keyword in the attributes.

For more information on the openCypher queries, visit:
    https://neo4j.com/docs/cypher-manual/5/introduction/
    https://neo4j.com/docs/getting-started/cypher-intro/

"""

import os
import sys
sys.path.append("/mnt/fs1/python") # import shapely library from AWS EFS

import time
from shapely.geometry import Point, LineString
from shapely.geometry.polygon import Polygon
from shapely.ops import nearest_points
from pyproj import Geod
from neo4j import GraphDatabase, RoutingControl

from set_impedance_factors import set_waze_impedance

class WazeAlertsQueries:

    def __init__(self, PARAMS, data, sidewalk_records, crosswalk_records):

        #print("Python Libraries on AWS EFS:", os.listdir("/mnt/fs1/python"))

        self.method = PARAMS["METHOD"] # API request method: POST, DELETE
        self.waze_holdtime = PARAMS["WAZE_HOLDTIME"]
        self.weather_distance = PARAMS["WEATHER_DISTANCE"]
        self.intersection_box = PARAMS["INTERSECTION_BOX"]
        self.crosswalk_box_outside = PARAMS["CROSSWALK_BOX_OUTSIDE"]
        self.crosswalk_box_outside_sidewalk = PARAMS["CROSSWALK_BOX_OUTSIDE_SIDEWALK"]
        self.crosswalk_box_inside = PARAMS["CROSSWALK_BOX_INSIDE"]
        self.meter_to_feet = PARAMS["METER_TO_FEET"]

        self.waze_data_fields = PARAMS["WAZE_DATA_FIELDS"]
        self.waze_alert_fields = PARAMS["WAZE_ALERT_FIELDS"]
        self.waze_node_attrs = PARAMS["WAZE_NODE_ATTRS"]
        self.waze_link_attrs = PARAMS["WAZE_LINK_ATTRS"]

        self.no_subtype = PARAMS["NO_SUBTYPE"]
        self.effect_type = PARAMS["EFFECT_TYPE"]
        self.hazard_weather = PARAMS["HAZARD_WEATHER"]
        self.jam = PARAMS["JAM"]
        self.waze_node_label = PARAMS["WAZE_NODE_LABEL"]
        self.osm_node_label = PARAMS["OSM_NODE_LABEL"]
        self.osm_way_label = PARAMS["OSM_WAY_LABEL"]
        self.sidewalk_label = PARAMS["SIDEWALK_LABEL"]
        self.crosswalk_label = PARAMS["CROSSWALK_LABEL"]

        self.data = data
        self.sidewalk_records = sidewalk_records # OSM-WAY nodes and their start/end OSM-NODE nodes
        self.crosswalk_records = crosswalk_records # OSM-WAY nodes and their start/end OSM-NODE nodes

        # define impedance factor or time delay for waze subtypes
        subtype_impedance = set_waze_impedance(self.sidewalk_label, self.crosswalk_label)
        self.subtype_impedance = subtype_impedance

        self.subtype_impedance_keys = self.subtype_impedance.keys()

        # set WGS84 geospatial CRS
        self.wgs84_geod = Geod(ellps = PARAMS["GEOSPACE"])

        # set up the python driver to send data to graph database
        self.URI = PARAMS["QUERY_URL"]
        print("Neptune database URI:", self.URI)

        self.AUTH = ("username", "password") # not used

    
    def check_existence(self, query, attrs=None):

        # check if a node type already exists in the database or not
        record, _, _ = self.driver.execute_query(
            query,
            parameters_=attrs or {},
            routing_=RoutingControl.READ,
        )

        return record

    
    def execute_query(self, query, attrs=None):

        # execute the query generated
        if attrs:
            self.driver.execute_query(query, parameters_=attrs)
        else:
            self.driver.execute_query(query, parameters_={})

        return
    

    def match_node(self, node_id_name, node_id, node_label):

        # write a query to determine if node exists
        query = "MATCH (node:`{}`) WHERE node.`{}` = $node_id RETURN node".format(
            node_label, node_id_name
        )

        # check if the node already exists or not
        record = self.check_existence(query, {"node_id": node_id})

        return record


    def create_node(self, node_label, attrs):

        # create node here
        query = "CREATE (n:`{}` $attrs)".format(node_label)

        self.execute_query(query, attrs)

        return


    def create_waze_node(self, alert, time_fields, datasetid):

        # ingest waze alert node to the AWS Neptune database
        location = alert[self.waze_alert_fields["location"]]

        # set up fields to be ingested for the waze alert node
        del alert[self.waze_alert_fields["location"]]
        attrs = {**alert, **time_fields}
        attrs[self.waze_node_attrs["location-x"]] = location[self.waze_alert_fields["x"]] # float lon: -84.25338
        attrs[self.waze_node_attrs["location-y"]] = location[self.waze_alert_fields["y"]] # float lat: 33.886974
        attrs[self.waze_node_attrs["__datasetid"]] = datasetid

        # ingest the node; NOTE: some fields may be in integer/float type
        self.create_node(self.waze_node_label, attrs)

        return
    

    def update_waze_endtimes(self, uuid, endtime_ms, endtime_timestamp):

        # update endtime property values
        query = "MATCH (waze:`{}`) WHERE waze.`{}` = $uuid SET waze.`{}` = $endtime_ms, waze.`{}` = $endtime".format(
            self.waze_node_label, self.waze_alert_fields["uuid"],
            self.waze_alert_fields["endTimeMillis"], self.waze_alert_fields["endTime"]
        )

        self.execute_query(query, {"uuid": uuid, "endtime_ms": endtime_ms, "endtime": endtime_timestamp})

        return
    

    def find_waze_impedance(self, node_label, wazetype, subtype):

        # find impedance factor and effect type of the relationship based on the waze subtype
        factor = 0
        effect_type = ""

        node_effect_type_key = node_label + "_" + self.effect_type # "SIDEWALK_EFFECT_TYPE" or "CROSSWALK_EFFECT_TYPE"
        no_subtype_key = self.no_subtype

        if subtype == "":

            # use waze type to look up the factor and effect type
            if wazetype in self.subtype_impedance_keys:

                factors_types = self.subtype_impedance[wazetype][no_subtype_key]

                if node_label in factors_types.keys():

                    # extract the factor and effect type for the waze no subtype and sidewalk/crosswalk node
                    factor = factors_types[node_label]
                    effect_type = factors_types[node_effect_type_key]

        else:

            if subtype in self.subtype_impedance_keys:

                factors_types = self.subtype_impedance[subtype]

                if node_label in factors_types.keys():

                    # extract the factor and effect type for the waze subtype and sidewalk/crosswalk node
                    factor = factors_types[node_label]
                    effect_type = factors_types[node_effect_type_key]

        return factor, effect_type
    

    def create_relationship(self, osm_node, osm_node_type, uuid, wazetype, subtype):

        # determine impedance factor and effect type to include in the relationship
        factor, effect_type = self.find_waze_impedance(osm_node_type, wazetype, subtype)

        # create relationship between the waze and its closest sidewalk/crosswalk node found
        query1 = "MATCH (osm:`{}`), (waze:`{}`) WHERE osm.id = $osm_id AND waze.`{}` = $uuid ".format(
            self.osm_way_label, self.waze_node_label, self.waze_alert_fields["uuid"]
        )
        
        #query2 = "CREATE (osm)-[r:`{}` {{__datasetid: '{}', __impedance_factor: {}, __impedance_effect_type: '{}'}}]->(waze)".\
        #    format(self.waze_node_label, osm_node["__datasetid"], factor, effect_type)

        query2 = "CREATE (osm)-[r:`{}` {{{}: $dataset_id, {}: $factor, {}: $effect_type}}]->(waze)".\
            format(self.waze_node_label, self.waze_link_attrs["__datasetid"], 
                   osm_node["__datasetid"], self.waze_link_attrs["__impedance_factor"], 
                   factor, self.waze_link_attrs["__impedance_effect_type"], effect_type)

        query = query1 + query2

        self.execute_query(query, {"osm_id": osm_node["id"], "uuid": uuid,
                                    "dataset_id": osm_node["__datasetid"],
                                    "factor": factor, "effect_type": effect_type})

        return
    
    
    def detach_waze_node(self, uuid):

        # delete waze node and link based on the uuid
        query1 = "MATCH (waze:`{}`) WHERE waze.`{}` = $uuid ".format(
            self.waze_node_label, self.waze_alert_fields["uuid"]
        )
        query2 = "DETACH DELETE waze"
        query = query1 + query2

        self.driver.execute_query(query, parameters_={"uuid": uuid})

        return
    

    def create_update_waze_node_link(self, osm_node, osm_node_type, alert, time_fields):

        # if the relationship exists where the attached waze has the same subtype, 
        # update the waze node with the one that has the lastest endtime, detach the old one
        # if the relationship doesn't exist, create the waze node and create the link
        uuid = alert[self.waze_alert_fields["uuid"]]
        wazetype = alert[self.waze_alert_fields["type"]]
        subtype = alert[self.waze_alert_fields["subtype"]]
        endtime = time_fields[self.waze_alert_fields["endTimeMillis"]]

        # check if the relationship exists between sidewalk/crosswalk and waze nodes that has the same waze subtype as the current waze node
        if subtype == "": # NO_SUBTYPE

            # use waze type to check the relationship where subtype is empty
            query = "MATCH (osm:`{}`)-[r:`{}`]->(waze:`{}`) " \
                "WHERE osm.id = $osm_id AND waze.`{}` = $subtype " \
                "AND waze.`{}` = $waze_type RETURN r, waze.`{}`, waze.`{}` LIMIT 1".format(
                    self.osm_way_label, self.waze_node_label, self.waze_node_label,
                    self.waze_alert_fields["subtype"], self.waze_alert_fields["type"],
                    self.waze_alert_fields["uuid"], self.waze_alert_fields["endTimeMillis"]
                )
            query_attrs = {"osm_id": osm_node["id"], "subtype": subtype, "waze_type": wazetype}

        else:

            # use waze subtype to check the relationship only
            query = "MATCH (osm:`{}`)-[r:`{}`]->(waze:`{}`) " \
                "WHERE osm.id = $osm_id AND waze.`{}` = $subtype " \
                "RETURN r, waze.`{}`, waze.`{}` LIMIT 1".format(
                    self.osm_way_label, self.waze_node_label, self.waze_node_label,
                    self.waze_alert_fields["subtype"], self.waze_alert_fields["uuid"],
                    self.waze_alert_fields["endTimeMillis"]
                )
            query_attrs = {"osm_id": osm_node["id"], "subtype": subtype}

        # check if the relationship already exists or not
        record = self.check_existence(query, query_attrs)

        if record:

            # ensure the waze node attached has the latest endTimeMillis
            attached_uuid = record[0]["waze." + self.waze_alert_fields["uuid"]]
            attached_endtime = record[0]["waze." + self.waze_alert_fields["endTimeMillis"]]

            if attached_endtime < endtime:

                # check to see if the waze node exists already in the database
                matched_node = self.match_node(self.waze_alert_fields["uuid"], uuid, self.waze_node_label)

                # create the waze node and create the relationship between the sidewalk/crosswalk and the waze node
                if not matched_node:

                    print("CREATE NEW WAZE NODE AND RELATIONSHIP TO REPLACE THE OLD ONE:", uuid)
                    self.create_waze_node(alert, time_fields, osm_node[self.waze_node_attrs["__datasetid"]])

                else:

                    print("CREATE NEW RELATIONSHIP WITH THE WAZE NODE CREATED TO REPLACE THE OLD ONE")
                
                self.create_relationship(osm_node, osm_node_type, uuid, wazetype, subtype)

                # detach the old waze node with the same subtype as the current waze node
                self.detach_waze_node(attached_uuid)

                print("OLD WAZE NODE {} DETACHED".format(attached_uuid))

        else:

            # check to see if the waze node exists already in the database
            matched_node = self.match_node(self.waze_alert_fields["uuid"], uuid, self.waze_node_label)

            # create the waze node and create the relationship between the sidewalk/crosswalk and the waze node
            if not matched_node:

                print("CREATE NEW WAZE NODE AND RELATIONSHIP:", uuid)
                self.create_waze_node(alert, time_fields, osm_node[self.waze_node_attrs["__datasetid"]])
            
            else:

                print("CREATE NEW RELATIONSHIP WITH THE WAZE NODE CREATED")

            self.create_relationship(osm_node, osm_node_type, uuid, wazetype, subtype)

        return
    

    def sort_sidewalks_crosswalks(self, sidewalk_nodes, sidewalk_distances, 
                                  crosswalk_nodes, crosswalk_distances,
                                  first_crosswalk_latlon, last_crosswalk_latlon):
        
        # order the sidewalk nodes found based on the computed distances
        sorted_sidewalk_nodes = [val for (_, val) in sorted(zip(sidewalk_distances, sidewalk_nodes), key=lambda x: x[0])]
        sorted_sidewalk_distances = sorted(sidewalk_distances)
        print("SORTED SIDEWALK DISTANCES:", sorted_sidewalk_distances)
        
        # order the crosswalk nodes found based on the computed distances
        sorted_crosswalk_nodes = [val for (_, val) in sorted(zip(crosswalk_distances, crosswalk_nodes), key=lambda x: x[0])]
        sorted_crosswalk_distances = sorted(crosswalk_distances)
        print("SORTED CROSSWALK DISTANCES:", sorted_crosswalk_distances)

        # order the first and last points of the crosswalk nodes based on the computed distances
        sorted_first_crosswalk_latlon = [val for (_, val) in sorted(zip(crosswalk_distances, first_crosswalk_latlon), key=lambda x: x[0])]
        sorted_last_crosswalk_latlon = [val for (_, val) in sorted(zip(crosswalk_distances, last_crosswalk_latlon), key=lambda x: x[0])]

        return sorted_sidewalk_nodes, sorted_sidewalk_distances, sorted_crosswalk_nodes, sorted_crosswalk_distances, \
            sorted_first_crosswalk_latlon, sorted_last_crosswalk_latlon


    def sort_sidewalk_crosswalk_nodes(self, alert):

        # order sidewalk and crosswalk nodes w.r.t. the current waze node location
        lat = alert[self.waze_alert_fields["location"]][self.waze_alert_fields["y"]] # lat is y
        lon = alert[self.waze_alert_fields["location"]][self.waze_alert_fields["x"]] # lon is x
        #waze_coords = (lat, lon)
        waze_coords = Point(lat, lon)

        sidewalk_nodes = []
        sidewalk_distances = []
        crosswalk_nodes = []
        crosswalk_distances = []

        first_crosswalk_latlon = []
        last_crosswalk_latlon = []

        for sidewalk_record in self.sidewalk_records:

            sidewalk = sidewalk_record.data()["sidewalk"]

            # find start and end points in lat/lon of the sidewalk
            start_node = sidewalk_record.data()["node1"]
            start_node_lat = start_node["lat"]
            start_node_lon = start_node["lon"]

            end_node = sidewalk_record.data()["node2"]
            end_node_lat = end_node["lat"]
            end_node_lon = end_node["lon"]

            # create the sidewalk line
            sidewalk_line = LineString(((start_node_lat, start_node_lon), (end_node_lat, end_node_lon)))

            # find the nearest point on the sidewalk line to the waze alert
            near_points = nearest_points(sidewalk_line, waze_coords)

            # determine the distance in meters between the two points
            lat1 = near_points[0].x # y if use (lon, lat) as inputs to Point and LineString above
            lon1 = near_points[0].y # x if use (lon, lat) as inputs to Point and LineString above
            lat2 = near_points[1].x # y if use (lon, lat) as inputs to Point and LineString above
            lon2 = near_points[1].y # x if use (lon, lat) as inputs to Point and LineString above
            _, _, dist = self.wgs84_geod.inv(lon1, lat1, lon2, lat2)

            # convert the distance from meters to feet
            distance = self.meter_to_feet * dist

            # store the sidewalk node and the computed distance
            sidewalk_nodes.append(sidewalk)
            sidewalk_distances.append(distance)

        for crosswalk_record in self.crosswalk_records:

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

            # find the nearest point on the crosswalk line to the waze alert
            near_points = nearest_points(crosswalk_line, waze_coords)

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

        # sort the sidewalk and crosswalk nodes based on the distances computed
        sorted_sidewalk_nodes, sorted_sidewalk_distances, \
        sorted_crosswalk_nodes, sorted_crosswalk_distances, \
        sorted_first_crosswalk_latlon, sorted_last_crosswalk_latlon \
            = self.sort_sidewalks_crosswalks(sidewalk_nodes, sidewalk_distances, 
                                             crosswalk_nodes, crosswalk_distances,
                                             first_crosswalk_latlon, last_crosswalk_latlon)
        
        return sorted_sidewalk_nodes, sorted_sidewalk_distances, sorted_crosswalk_nodes, sorted_crosswalk_distances, \
            sorted_first_crosswalk_latlon, sorted_last_crosswalk_latlon


    def find_sidewalk_crosswalk_nodes_weather(self, sorted_sidewalk_nodes, sorted_sidewalk_distances,
                                              sorted_crosswalk_nodes, sorted_crosswalk_distances):

        # find all sidewalk and crosswalk nodes within 1,000 ft of the waze node for waze attachment
        attachment_counts = len(list(filter(lambda distance: distance <= self.weather_distance, sorted_sidewalk_distances)))
        node_attachments = sorted_sidewalk_nodes[:attachment_counts]
        node_attachment_types = [self.sidewalk_label] * attachment_counts

        attachment_counts = len(list(filter(lambda distance: distance <= self.weather_distance, sorted_crosswalk_distances)))
        node_attachments += sorted_crosswalk_nodes[:attachment_counts]
        node_attachment_types += [self.crosswalk_label] * attachment_counts

        return node_attachments, node_attachment_types

    
    def find_sidewalk_crosswalk_node_outbox(self, sorted_sidewalk_nodes, sorted_sidewalk_distances,
                                            sorted_crosswalk_nodes, sorted_crosswalk_distances):

        # find nearest crosswalk node that is within 80 ft of the waze node if exists
        # else find all the sidewalk nodes that are within 50 ft
        node_attachments = [] # contains a single crosswalk or multiple sidewalk nodes
        node_attachment_types = []

        if sorted_crosswalk_nodes:

            crosswalk_node = sorted_crosswalk_nodes[0]
            crosswalk_distance = sorted_crosswalk_distances[0]

            # check if the nearest crosswalk node is within 80 ft
            if crosswalk_distance <= self.crosswalk_box_outside:

                # attach the crosswalk node as waze attachment
                node_attachments.append(crosswalk_node)
                node_attachment_types.append(self.crosswalk_label)

        if not node_attachments:

            # find all sidewalk nodes within 50 ft of the waze node for waze attachment
            attachment_counts = len(list(filter(lambda distance: distance <= self.crosswalk_box_outside_sidewalk, 
                                                sorted_sidewalk_distances)))
            node_attachments = sorted_sidewalk_nodes[:attachment_counts]
            node_attachment_types = [self.sidewalk_label] * attachment_counts

        return node_attachments, node_attachment_types
    

    def find_square_box_points(self, close_first_crosswalk_latlon, close_last_crosswalk_latlon,
                               far_first_crosswalk_latlon, far_last_crosswalk_latlon):

        # form the square based on the 4 points found
        points = [close_first_crosswalk_latlon, close_last_crosswalk_latlon,
                  far_first_crosswalk_latlon, far_last_crosswalk_latlon]
        square = Polygon(points)

        return square


    def check_waze_intersection(self, alert, sorted_crosswalk_nodes, sorted_crosswalk_distances,
                                sorted_first_crosswalk_latlon, sorted_last_crosswalk_latlon):

        inbox = False

        if len(sorted_crosswalk_nodes) >= 4: # sufficient to check 4 closest crosswalk nodes

            # check to see if the 4th closest crosswalk is within 300 ft 
            crosswalk_distance_check = sorted_crosswalk_distances[3]

            if crosswalk_distance_check <= self.intersection_box:

                # extract the start and end points lat/lon of the closest and the fartherest (4th) crosswalks
                close_first_crosswalk_latlon = sorted_first_crosswalk_latlon[0]
                close_last_crosswalk_latlon = sorted_last_crosswalk_latlon[0]
                far_first_crosswalk_latlon = sorted_first_crosswalk_latlon[3]
                far_last_crosswalk_latlon = sorted_last_crosswalk_latlon[3]

                # draw a rough intersection box to check if waze alert is in the box
                square = self.find_square_box_points(close_first_crosswalk_latlon, close_last_crosswalk_latlon,
                                                     far_first_crosswalk_latlon, far_last_crosswalk_latlon)

                # check to see if the waze node is in the square box
                lat = alert[self.waze_alert_fields["location"]][self.waze_alert_fields["y"]] # lat is y
                lon = alert[self.waze_alert_fields["location"]][self.waze_alert_fields["x"]] # lon is x
                waze_point = Point(lat, lon)
                inbox = square.contains(waze_point)

        return inbox
    

    def find_crosswalk_nodes_inbox(self, sorted_crosswalk_nodes, sorted_crosswalk_distances):

        # find possibly crosswalk node(s) that are within 20 ft of the waze node,
        # else find the nearest crosswalk node
        node_attachments = [] # contain a single or multiple crosswalk node(s)
        node_attachment_types = []

        attachment_counts = len(list(filter(lambda distance: distance <= self.crosswalk_box_inside, sorted_crosswalk_distances)))
        node_attachments = sorted_crosswalk_nodes[:attachment_counts]
        node_attachment_types = [self.crosswalk_label] * attachment_counts

        if not node_attachments:

            # attach the nearest crosswalk node as waze attachment
            if sorted_crosswalk_nodes:
                node_attachments.append(sorted_crosswalk_nodes[0])
                node_attachment_types.append(self.crosswalk_label)

        return node_attachments, node_attachment_types
    

    def generate_post_query(self):

        # parse each alert in the waze data to add them as nodes and create links
        alerts = self.data[self.waze_data_fields["alerts"]]
        starttime_ms = self.data[self.waze_data_fields["startTimeMillis"]]
        endtime_ms = self.data[self.waze_data_fields["endTimeMillis"]]
        starttime_timestamp = self.data[self.waze_data_fields["startTime"]] # str
        endtime_timestamp = self.data[self.waze_data_fields["endTime"]] # str
        time_fields = {self.waze_data_fields["startTimeMillis"]: starttime_ms, 
                       self.waze_data_fields["endTimeMillis"]: endtime_ms, 
                       self.waze_data_fields["startTime"]: starttime_timestamp, 
                       self.waze_data_fields["endTime"]: endtime_timestamp}
        
        # ingest or update waze alert nodes
        for alert in alerts:

            if self.waze_alert_fields["type"] not in alert.keys() \
                or self.waze_alert_fields["subtype"] not in alert.keys() \
                    or self.waze_alert_fields["roadType"] not in alert.keys():

                print("WARNING: THE CURRENT WAZE ALERT DOES NOT CONTAIN POSSIBLE DATA:", alert)
                continue # the alert cannot be processed without a type, subtype or roadType

            if alert[self.waze_alert_fields["type"]] == self.jam:

                print("THE CURRENT WAZE ALERT REPRESENTS A JAM AND IS DISCARDED:", alert)
                continue # waze alert represents a jam and is discarded

            if alert[self.waze_alert_fields["roadType"]] == 3:
                
                print("THE CURRENT WAZE ALERT REPRESENTS A FREEWAY AND IS DISCARDED:", alert)
                continue # waze alert represents a freeway and is discarded
            
            # determine if the waze node exists with uuid and label WAZE-ALERT
            uuid = alert[self.waze_alert_fields["uuid"]]
            matched_node = self.match_node(self.waze_alert_fields["uuid"], uuid, self.waze_node_label)
            subtype = alert[self.waze_alert_fields["subtype"]]

            if matched_node:

                # update endtime fields of the waze node if the current waze has later endtimes
                attached_endtime = matched_node[0].data()["node"][self.waze_data_fields["endTimeMillis"]]
                if attached_endtime < endtime_ms:
                    self.update_waze_endtimes(uuid, endtime_ms, endtime_timestamp)
                    print("WAZE NODE UUID {} IS FOUND AND ITS ENDTIME FIELDS UPDATED".format(uuid))
            
            else:

                # find and sort sidewalk and crosswalk nodes w.r.t. the current waze node first
                sorted_sidewalk_nodes, sorted_sidewalk_distances, \
                sorted_crosswalk_nodes, sorted_crosswalk_distances, \
                sorted_first_crosswalk_latlon, sorted_last_crosswalk_latlon \
                    = self.sort_sidewalk_crosswalk_nodes(alert)
                
                if len(sorted_sidewalk_distances) == 0 or len(sorted_crosswalk_distances) == 0:
                    print("WARNING: NO SIDEWALKS OR CROSSWALKS ARE FOUND.")
                    continue
                
                # discard the waze alert if closest sidewalk and crosswalk nodes are farther than 1,000 ft away
                if sorted_sidewalk_distances[0] > self.weather_distance and sorted_crosswalk_distances[0] > self.weather_distance:

                    print("THE CURRENT WAZE ALERT IS TOO FAR AWAY FROM ANY SIDEWALK/CROSSWALK AND IS DISCARDED:", alert)
                    continue # waze alert is more than 1,000 ft away from its closest sidewalk/crosswalk and is discarded

                # find potential sidewalk and crosswalk node(s) for waze node attachments
                hazard_weather = self.hazard_weather + "_"
                if subtype.startswith(hazard_weather):

                    # find all sidewalk and crosswalk nodes within 1,000 ft of the waze node
                    node_attachments, node_attachment_types = self.find_sidewalk_crosswalk_nodes_weather(
                        sorted_sidewalk_nodes, 
                        sorted_sidewalk_distances,
                        sorted_crosswalk_nodes,
                        sorted_crosswalk_distances)

                else:

                    # determine if the waze alert is within an intersection box
                    inbox = self.check_waze_intersection(alert, sorted_crosswalk_nodes,
                                                         sorted_crosswalk_distances,
                                                         sorted_first_crosswalk_latlon,
                                                         sorted_last_crosswalk_latlon)

                    if inbox:

                        # find possibly crosswalk node(s) that are within 20 ft of the waze node,
                        # else find the nearest crosswalk node
                        node_attachments, node_attachment_types = self.find_crosswalk_nodes_inbox(
                            sorted_crosswalk_nodes,
                            sorted_crosswalk_distances)

                    else:

                        # find possibly nearest crosswalk node that is within 80 ft of the waze node,
                        # else find all the sidewalk nodes that are within 50 ft
                        node_attachments, node_attachment_types = self.find_sidewalk_crosswalk_node_outbox(
                            sorted_sidewalk_nodes,
                            sorted_sidewalk_distances,
                            sorted_crosswalk_nodes,
                            sorted_crosswalk_distances)

                print("NODE ATTACHMENTS:", node_attachments)

                # attach waze node to each sidewalk/crosswalk nodes found if possible
                count = 0
                for osm_node in node_attachments:

                    # create or update waze node or link based on the current waze subtype
                    osm_node_type = node_attachment_types[count] # "SIDEWALK" or "CROSSWALK"
                    self.create_update_waze_node_link(osm_node, osm_node_type, alert, time_fields)
                    count += 1

        return
    

    def generate_delete_query(self):

        # delete waze alert nodes and links based on their last update time
        current_time_ms = round(time.time() * 1000)
        time_limit = current_time_ms - self.waze_holdtime 
        
        # delete waze nodes and links that have last update time less than the time limit; endTimeMillis in EST/EDT
        query1 = "MATCH (waze:`{}`) WHERE waze.{} < {} ".format(self.waze_node_label, 
                                                           self.waze_alert_fields["endTimeMillis"], 
                                                           time_limit)
        query2 = "DETACH DELETE waze"
        query = query1 + query2

        self.driver.execute_query(query)

        return


    def create_transaction(self):

        with GraphDatabase.driver(self.URI, auth=self.AUTH, encrypted=True) as self.driver:

            if self.method == "POST":
                self.generate_post_query()
            elif self.method == "DELETE":
                self.generate_delete_query()
            else:
                message = "ERROR: Request method {} is not supported. Choose POST or DELETE.".format(self.method)
                print(message)

        return
