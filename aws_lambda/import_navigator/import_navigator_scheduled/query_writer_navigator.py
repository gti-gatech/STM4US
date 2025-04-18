"""
The script consists of openCypher queries for creating/deleting nodes and links for 
NaviGAtor scheduled and unscheduled events data and ingest them to an AWS Neptune database with AWS EventBridge scheduler::
    "bolt://<database-name.cluster-id>.us-east-2.neptune.amazonaws.com:8182"

For more information on the openCypher queries, visit:
    https://neo4j.com/docs/cypher-manual/5/introduction/
    https://neo4j.com/docs/getting-started/cypher-intro/

"""

import os
import sys
sys.path.append("/mnt/fs2") # import shapely library from AWS EFS

import time
from shapely.geometry import Point, LineString
from shapely.geometry.polygon import Polygon
from shapely.ops import nearest_points
from pyproj import Geod
from neo4j import GraphDatabase, RoutingControl

from set_impedance_factors import set_unscheduled_events_impedance, set_scheduled_events_impedance


class NavigatorEventQueries:

    def __init__(self, query_url, method, scheduled_events, unscheduled_events, 
                 comments, properties, sidewalk_records, crosswalk_records, 
                 data_set_id):
        
        print("Python Libraries on AWS EFS:", os.listdir("/mnt/fs2"))

        self.method = method # API request method: POST, DELETE
        self.scheduled_events = scheduled_events # dataframe
        self.unscheduled_events = unscheduled_events # dataframe
        self.comments = comments # dataframe
        self.properties = properties # dataframe
        self.sidewalk_records = sidewalk_records # OSM-WAY nodes and their start/end OSM-NODE nodes
        self.crosswalk_records = crosswalk_records # OSM-WAY nodes and their start/end OSM-NODE nodes
        self.datasetid = data_set_id # a grid cell name, e.g. 34.0N84.4W
        self.event_holdtime = 900000 # holding event data for 15 minutes or 900000 ms for DELETE request
        self.attach_radius = 50.0 # distance (ft) boundary for sidewalk/crosswalk node attachments
        self.meter_to_feet = 3.28084 # 1 meter is 3.28084 feet

        # set WGS84 geospatial CRS
        self.wgs84_geod = Geod(ellps = "WGS84")

        if self.method == "POST":
            
            # define the coordinate grids of the study area
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

            # set up the square of the current grid cell
            grid = self.study_area[self.datasetid]
            grid_points = [(grid["min_lat"], grid["min_lon"]), (grid["max_lat"], grid["min_lon"]),
                           (grid["max_lat"], grid["max_lon"]), (grid["min_lat"], grid["max_lon"])]
            self.square = Polygon(grid_points)

        self.event_node_label = "NAVIGATOR-EVENT"
        self.comment_node_label = "NAVIGATOR-EVENT-COMMENT"
        self.property_node_label = "NAVIGATOR-EVENT-PROPERTY"
        self.osm_node_label = "OSM-NODE"
        self.osm_way_label = "OSM-WAY"
        self.sidewalk_label = "SIDEWALK"
        self.crosswalk_label = "CROSSWALK"

        # set up the python driver to send data to graph database
        self.URI = query_url

        print("Neptune database URI:", self.URI)

        self.AUTH = ("username", "password") # not used

    
    def check_existence(self, query):

        # check if a node type already exists in the database or not
        record, _, _ = self.driver.execute_query(
            query,
            routing_=RoutingControl.READ,
        )

        return record

    
    def execute_query(self, query, attrs=None):

        # execute the query generated
        if attrs:
            self.driver.execute_query(query, attrs=attrs)
        else:
            self.driver.execute_query(query)

        return
    

    def match_node(self, node_id_name, node_id, node_label):

        # write a query to determine if node exists
        query = "MATCH (node:`{}`) WHERE node.`{}` = '{}' RETURN node".format(node_label, node_id_name, node_id)

        # check if the node already exists or not
        record = self.check_existence(query)

        return record
    

    def match_event_comment_relationship(self, event_id):

        # find the comment node attached with the event node if exist
        query = "MATCH (event:`{}`)-[r:`{}`]->(comment:`{}`) WHERE event.event_id = '{}' RETURN comment".\
            format(self.event_node_label, self.event_node_label, self.comment_node_label, event_id)
        
        # check if the comment node already exists or not
        record = self.check_existence(query)

        return record
    

    def match_event_property_relationship(self, event_id):

        # find the property nodes attached with the event node if exist
        query = "MATCH (event:`{}`)-[r:`{}`]->(property:`{}`) WHERE event.event_id = '{}' RETURN property".\
            format(self.event_node_label, self.event_node_label, self.property_node_label, event_id)
        
        # check if the property nodes already exist or not
        record = self.check_existence(query)

        return record
    

    def events_in_square_box(self, events):

        def is_in_square_box(row):
            # determine if the point is in the square based on its lat and lon
            inbox = self.square.contains(Point(float(row["latitude"]), float(row["longitude"])))
            return inbox

        # call is_in_square_box to filter out events that are outside of the square grid
        inbox = events.apply(is_in_square_box, axis=1)
        events = events[inbox]

        return events
    

    def filter_events(self, events):

        # filter out events that are outside of the current grid cell
        events = self.events_in_square_box(events)

        # filter out events created by Waze and road type Interstate
        events = events.loc[(events["created_by"] != "Waze") & (events["road_type"] != "Interstate")]
        
        # filter out duplicated rows and retain the ones with larger version
        events = events.sort_values(by = ["version"], ascending = False)

        # drop all duplicated rows using columns 'event_id' and 'external_id'; single event returned
        events = events.drop_duplicates(subset = ["event_id", "external_id"], keep = "first")

        return events
    

    def filter_comments(self, event_id):

        # find comments that are associated with the current event node
        comments = self.comments.loc[self.comments["event_id"] == event_id]

        return comments
    

    def filter_properties(self, event_id):

        # find properties that have the same event_id with the current event node
        properties = self.properties.loc[self.properties["event_id"] == event_id]

        # filter out the properties that have type Waze
        properties = properties.loc[properties["type"] != "Waze"]

        # sort the properties based on version
        properties = properties.sort_values(by = ["version"], ascending = False)

        # retain the properties that has the latest version; multiple properties for the event possible
        properties = properties.drop_duplicates(subset = ["property_id"], keep = "first")

        return properties
    

    def update_event_version(self, event_id, version):

        # update version property value for the event node
        query = "MATCH (event:`{}`) WHERE event.event_id = '{}' SET event.version = {}".\
            format(self.event_node_label, event_id, version)

        self.execute_query(query)

        return
    

    def sort_sidewalk_crosswalk_nodes(self, event):

        # order sidewalk and crosswalk nodes w.r.t. the current event node location
        lat = float(event["latitude"])
        lon = float(event["longitude"])
        # event_coords = (lat, lon)
        event_coords = Point(lat, lon)

        sidewalk_nodes = []
        sidewalk_distances = []
        crosswalk_nodes = []
        crosswalk_distances = []

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
            near_points = nearest_points(sidewalk_line, event_coords)

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
            near_points = nearest_points(crosswalk_line, event_coords)

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

        # order the sidewalk nodes found based on the computed distances
        sorted_sidewalk_nodes = [val for (_, val) in sorted(zip(sidewalk_distances, sidewalk_nodes), key=lambda x: x[0])]
        sorted_sidewalk_distances = sorted(sidewalk_distances)
        
        # order the crosswalk nodes found based on the computed distances
        sorted_crosswalk_nodes = [val for (_, val) in sorted(zip(crosswalk_distances, crosswalk_nodes), key=lambda x: x[0])]
        sorted_crosswalk_distances = sorted(crosswalk_distances)

        return sorted_sidewalk_nodes, sorted_sidewalk_distances, sorted_crosswalk_nodes, sorted_crosswalk_distances
    

    def create_node(self, node_label, attrs):

        # create node here
        query = "CREATE (n:`{}` $attrs)".format(node_label)

        self.execute_query(query, attrs)

        return
    

    def create_event_node(self, event):

        # create unscheduled event node in the AWS Neptune database
        attrs = event.to_dict()
        attrs["__datasetid"] = self.datasetid

        # ingest the node; NOTE: some fields may be in integer/float type
        self.create_node(self.event_node_label, attrs)

        return
    

    def create_comment_node(self, comment):

        # create comment node in the AWS Neptune database
        attrs = comment.to_dict()
        attrs["__datasetid"] = self.datasetid

        # ingest the node
        self.create_node(self.comment_node_label, attrs)

        return
    

    def create_property_node(self, property):

        # create property node in the AWS Neptune database
        attrs = property.to_dict()
        attrs["__datasetid"] = self.datasetid

        # ingest the node
        self.create_node(self.property_node_label, attrs)

        return
    

    def create_osm_event_relationship(self, osm_node, event_id, factor, effect_type):

        # create relationship between the event node and the sidewalk/crosswalk node found
        query1 = "MATCH (osm:`{}`), (event:`{}`) WHERE osm.id = '{}' AND event.event_id = '{}' ".\
            format(self.osm_way_label, self.event_node_label, osm_node["id"], event_id)
        query2 = "CREATE (osm)-[r:`{}` {{__datasetid: '{}', __impedance_factor: {}, __impedance_effect_type: '{}'}}]->(event)".\
            format(self.event_node_label, osm_node["__datasetid"], factor, effect_type)

        query = query1 + query2

        self.execute_query(query)

        return
    

    def create_event_comment_relationship(self, event_id, comment_id, datasetid):

        # create relationship between the event node and the comment node found
        query1 = "MATCH (event:`{}`), (comment:`{}`) WHERE event.event_id = '{}' AND comment.comment_id = '{}' ".\
            format(self.event_node_label, self.comment_node_label, event_id, comment_id)
        query2 = "CREATE (event)-[r:`{}` {{__datasetid: '{}'}}]->(comment)".\
            format(self.event_node_label, datasetid)

        query = query1 + query2

        self.execute_query(query)

        return
    

    def create_event_property_relationship(self, event_id, property_id, datasetid):

        # create relationship between the event node and the property node found
        query1 = "MATCH (event:`{}`), (property:`{}`) WHERE event.event_id = '{}' AND property.property_id = '{}' ".\
            format(self.event_node_label, self.property_node_label, event_id, property_id)
        query2 = "CREATE (event)-[r:`{}` {{__datasetid: '{}'}}]->(property)".\
            format(self.event_node_label, datasetid)

        query = query1 + query2

        self.execute_query(query)


        return
    

    def detach_comment_node(self, comment_id):

        # delete comment node and link based on the comment_id
        query1 = "MATCH (comment:`{}`) WHERE comment.comment_id = '{}' ".format(self.comment_node_label, comment_id)
        query2 = "DETACH DELETE comment"
        query = query1 + query2

        self.driver.execute_query(query)

        return
    

    def detach_property_node(self, property_id):

        # delete property node and link based on the property_id
        query1 = "MATCH (property:`{}`) WHERE property.property_id = '{}' ".format(self.property_node_label, property_id)
        query2 = "DETACH DELETE property"
        query = query1 + query2

        self.driver.execute_query(query)

        return
    

    def create_attach_comment_node(self, matched_node, matched_comments, comments):

        # create and attach the new comment nodes found
        event_id = matched_node[0].data()["node"]["event_id"]
        datasetid = matched_node[0].data()["node"]["__datasetid"]

        # create and attach the new comment nodes found
        if matched_comments: # there are comment nodes already attached with the event node

            matched_comment_lookup = []

            # extract attached comment node id
            for matched_comment in matched_comments:

                comment = matched_comment.data()["comment"]
                comment_id = comment["comment_id"]

                matched_comment_lookup.append(comment_id)

            for _, comment in comments.iterrows():

                # check if the comment node is already attached with the event node
                comment_id = comment["comment_id"]

                if comment_id not in matched_comment_lookup:

                    # create and attach the new comment found
                    self.create_comment_node(comment)
                    self.create_event_comment_relationship(event_id, comment_id, datasetid)

        else: # there are no comment nodes currently attached with the event node

            # create and attach the new comment nodes found
            for _, comment in comments.iterrows():

                # create and attach the new comment node
                self.create_comment_node(comment)

                comment_id = comment["comment_id"]
                self.create_event_comment_relationship(event_id, comment_id, datasetid)

        return
    

    def update_property_version(self, property_id, version):

        # update version value of the property node
        query = "MATCH (property:`{}`) WHERE property.property_id = '{}' SET property.version = {}".\
            format(self.property_node_label, property_id, version)

        self.execute_query(query)

        return
    

    def create_attach_property_node(self, matched_node, matched_properties, properties):

        # create and attach the new property nodes found
        event_id = matched_node[0].data()["node"]["event_id"]
        datasetid = matched_node[0].data()["node"]["__datasetid"]

        if matched_properties: # there are property nodes already attached with the event node

            matched_property_lookup = []

            # extract attached property node id and version
            for matched_property in matched_properties:

                property = matched_property.data()["property"]
                property_id = property["property_id"]
                version = property["version"]

                property_lookup = {"property_id": property_id, "version": version}
                matched_property_lookup.append(property_lookup)


            for _, property in properties.iterrows():

                # check if the property node is already attached with the event node
                property_id = property["property_id"]
                found_property = next((item for item in matched_property_lookup if item["property_id"] == property_id), None)
                if found_property:

                    matched_version = found_property["version"]

                    if matched_version < property["version"]:

                        # update version field of the property node
                        self.update_property_version(property_id, property["version"])
                
                else:

                    # create and attach the new property node
                    self.create_property_node(property)
                    self.create_event_property_relationship(event_id, property_id, datasetid)

        else: # there are no property nodes currently attached with the event node

            # create and attach the new property nodes found
            for _, property in properties.iterrows():

                # create and attach the new property node
                self.create_property_node(property)

                property_id = property["property_id"]
                self.create_event_property_relationship(event_id, property_id, datasetid)

        return
    

    def find_event_impedance(self, event, scheduled_unscheduled, sidewalk_crosswalk):

        # find impedance factor and effect type for the event
        if scheduled_unscheduled == "UNSCHEDULED":

            event_type = event["type"]
            event_severity = event["severity"]

            factor, effect_type = set_unscheduled_events_impedance(event_type, event_severity, 
                                                                   sidewalk_crosswalk)

        else:
            
            event_subtype = event["subtype"]
            factor, effect_type = set_scheduled_events_impedance(event_subtype)

        return factor, effect_type
    

    def create_event_node_relationships(self, event, scheduled_unscheduled,
                                        sorted_sidewalk_nodes, sorted_sidewalk_distances,
                                        sorted_crosswalk_nodes, sorted_crosswalk_distances):
        
        # track if event node is created
        event_node_created = False 

        # find all sidewalk and crosswalk nodes within 50 ft of the event node for event attachment
        attachment_counts = len(list(filter(lambda distance: distance <= self.attach_radius, sorted_sidewalk_distances)))
        node_attachments = sorted_sidewalk_nodes[:attachment_counts]
        node_attachment_types = [self.sidewalk_label] * attachment_counts

        attachment_counts = len(list(filter(lambda distance: distance <= self.attach_radius, sorted_crosswalk_distances)))
        node_attachments += sorted_crosswalk_nodes[:attachment_counts]
        node_attachment_types += [self.crosswalk_label] * attachment_counts

        # create event node and attach it with sidewalk/crosswalk nodes found
        if node_attachments:

            # create the new event node in the database
            self.create_event_node(event)
            event_node_created = True
            event_id = event["event_id"] # str
            print("EVENT NODE EVENT_ID {} IS CREATED".format(event_id))
            print("NODE ATTACHMENTS:", node_attachments)
            count = 0

            for osm_node in node_attachments:

                sidewalk_crosswalk = node_attachment_types[count] # "SIDEWALK" or "CROSSWALK"

                # find impedance factor and effect type for the event
                factor, effect_type = self.find_event_impedance(event, scheduled_unscheduled, sidewalk_crosswalk)

                # create the relationship between the event node and its closest sidewalk or crosswalk node found
                self.create_osm_event_relationship(osm_node, event_id, factor, effect_type)

                count += 1

        return event_node_created
    

    def create_attach_events(self, events, scheduled_unscheduled):

        # ingest scheduled/unscheduled events with comments and properties
        for _, event in events.iterrows():

            event_id = event["event_id"] # str
            version = event["version"] # int

            # check if the event already exists in the database
            matched_node = self.match_node("event_id", event_id, self.event_node_label)

            # find incoming comments to attach if exist
            comments = self.filter_comments(event_id)

            # find incoming properties that have the lastest version to attach if exist
            properties = self.filter_properties(event_id)

            if matched_node: # the event node already exists in the database

                # update version field of the event node if the current event has later version
                attached_version = matched_node[0].data()["node"]["version"] # int
                if attached_version < version:
                    self.update_event_version(event_id, version)
                    print("EVENT NODE EVENT_ID {} IS FOUND AND ITS VERSION FIELD UPDATED".format(event_id))

                # check if comment nodes are attached with the existing event node
                matched_comments = self.match_event_comment_relationship(event_id)

                # check if property nodes are attached with the existing event node
                matched_properties = self.match_event_property_relationship(event_id)

                if not comments.empty:

                    # create and attach the new comment nodes found
                    self.create_attach_comment_node(matched_node, matched_comments, comments)

                if not properties.empty:

                    # create and attach the new property nodes found; update old property nodes if needed
                    self.create_attach_property_node(matched_node, matched_properties, properties)

            else: # create new event node in the database

                # find and sort sidewalk and crosswalk nodes w.r.t. the current event node first
                sorted_sidewalk_nodes, sorted_sidewalk_distances, \
                sorted_crosswalk_nodes, sorted_crosswalk_distances \
                    = self.sort_sidewalk_crosswalk_nodes(event)
                
                # create and attach the new event node to sidewalk and crosswalk nodes
                event_node_created = self.create_event_node_relationships(event, scheduled_unscheduled,
                                                                          sorted_sidewalk_nodes, sorted_sidewalk_distances,
                                                                          sorted_crosswalk_nodes, sorted_crosswalk_distances)

                if event_node_created: # the new event node is created

                    # attach comment nodes
                    if not comments.empty:

                        # create and attach the comment nodes found with the event node
                        for _, comment in comments.iterrows():
                            
                            self.create_comment_node(comment)

                            comment_id = comment["comment_id"]
                            self.create_event_comment_relationship(event_id, comment_id, self.datasetid)

                    # attach property nodes
                    if not properties.empty:

                        # create and attach the new property nodes found with the event node
                        for _, property in properties.iterrows():

                            self.create_property_node(property)

                            property_id = property["property_id"]
                            self.create_event_property_relationship(event_id, property_id, self.datasetid)

        return


    def generate_post_query(self):

        # filter event rows that do not need to be processed
        if not self.unscheduled_events.empty:
            unscheduled_events = self.filter_events(self.unscheduled_events)

            # ingest unscheduled events with their comments and properties
            self.create_attach_events(unscheduled_events, "UNSCHEDULED")
        
        if not self.scheduled_events.empty:
            scheduled_events = self.filter_events(self.scheduled_events)

            # ingest scheduled events with their comments and properties
            self.create_attach_events(scheduled_events, "SCHEDULED")

        return
    

    def generate_delete_query(self):

        # delete event data nodes and links based on the last modified time
        current_time_ms = round(time.time() * 1000)
        time_limit = current_time_ms - self.event_holdtime 
        
        # delete event nodes and links that have last modified time less than the time limit; modified_date in EST/EDT
        query1 = "MATCH (event:`{}`)-[relation:`{}`]->(comment_property) WHERE event.__modified_date_ms < {} ".\
            format(self.event_node_label, self.event_node_label, time_limit)
        query2 = "DETACH DELETE event, relation, comment_property"
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
