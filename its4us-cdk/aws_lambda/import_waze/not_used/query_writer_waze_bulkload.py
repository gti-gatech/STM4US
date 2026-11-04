"""
The script consists of openCypher queries for creating/deleting nodes and links for 
Waze alerts data and ingest them to an AWS Neptune database using bulk load:
    "bolt://<database-name.cluster-id>.us-east-2.neptune.amazonaws.com:8182"

For more information on the openCypher queries, visit:
    https://neo4j.com/docs/cypher-manual/5/introduction/
    https://neo4j.com/docs/getting-started/cypher-intro/

"""

import time
from neo4j import GraphDatabase, RoutingControl


class WazeAlertsQueriesBulkLoad:

    def __init__(self, query_url, method, data, waze_nodes_bulkload, waze_relationships_bulkload):

        self.method = method # API request method: POST, DELETE
        self.data = data
        self.waze_holdtime = 900000 # holding waze data for 15 minutes or 900000 ms for DELETE request

        self.waze_node_label = "WAZE-ALERT"
        self.sidewalk_link_label = "GT/CE-SIDEWALK"

        # store waze data in list of dictionaries for bulk load
        self.waze_node_bulkload = waze_nodes_bulkload
        self.waze_relationship_bulkload = waze_relationships_bulkload

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
        query = "MATCH (n:`{}`) WHERE n.`{}` = '{}' RETURN n, n.endTimeMillis LIMIT 1".\
            format(node_label, node_id_name, node_id)

        # check if the node already exists or not
        record = self.check_existence(query)

        return record
    

    def match_node_bulkload(self, uuid):

        # find if the node already exists in self.waze_node_bulkload
        matched = False
        waze_found = {}

        for node in self.waze_node_bulkload:

            if node["~id"] == uuid:

                # extract node's endTimeMillis to check the timestamp
                waze_found["endTimeMillis"] = node["endTimeMillis:Long(single)"]
                matched = True
                break
        
        return matched, waze_found


    def create_waze_node(self, alert, time_fields, datasetid):

        # ingest waze alert node to the AWS Neptune database
        location = alert["location"]

        # set up fields to be ingested for the waze alert node
        del alert["location"]
        attrs = {**alert, **time_fields}
        attrs["location-x"] = location["x"] # float lon: -84.25338
        attrs["location-y"] = location["y"] # float lat: 33.886974

        waze_node = {"~id": alert["uuid"], "~label": self.waze_node_label, "__datasetid:String(single)": datasetid}

        for key, val in attrs.items():

            # default everything else to string to handle possible empty values
            name = key + ":String(single)"

            # treat location-x and location-y as float data type
            if key == "location-x" or key == "location-y":
                name = key + ":Float(single)"

            # treat endTimeMillis as long data type for bulk load and easier for deletion
            if key == "endTimeMillis":
                # make them long type for the bulk load, can't be int
                name = key + ":Long(single)"

            waze_node[name] = val

        # store the waze node for bulk load
        self.waze_node_bulkload.append(waze_node)

        return
    

    def update_waze_endtimes(self, uuid, endtime_ms, endtime_timestamp):

        # update endtime property values by saving the waze data as node
        #waze_node = {"~id": uuid, "~label": self.waze_node_label, "endTimeMillis:Long(single)": endtime_ms,
        #             "endTime:String(single)": endtime_timestamp}

        # store the waze node for bulk load
        #self.waze_node_bulkload.append(waze_node)
        
        # update endtime property values directly in the database
        query = "MATCH (waze:`{}`) WHERE waze.uuid = '{}' SET waze.endTimeMillis = {}, waze.endTime = '{}'".\
            format(self.waze_node_label, uuid, endtime_ms, endtime_timestamp)
        
        self.execute_query(query)

        return
    

    def find_sidewalk_node(self, alert):

        # compute the aggregated distances between all GT/CE-SIDEWALK nodes and the current waze alert node
        lat = alert["location"]["y"] # lat is y
        lon = alert["location"]["x"] # lon is x

        query1 = "MATCH (sidewalk:`{}`) WITH sidewalk, abs(sidewalk.sidewalksimLinkCentroidLatitude - {}) as latDiff, ".\
            format(self.sidewalk_link_label, lat)
        query2 = "abs(sidewalk.sidewalksimLinkCentroidLongitude - {}) as lonDiff ".format(lon)
        query3 = "SET sidewalk.__wazedistance = latDiff + lonDiff"
        
        query = query1 + query2 + query3

        self.execute_query(query)

        # find the GT/CE-SIDEWALK node that is closest to the current waze alert node
        query = "MATCH (sidewalk:`{}`) WITH sidewalk, sidewalk.__wazedistance as wazedistance " \
            + "ORDER BY wazedistance ASC RETURN ID(sidewalk), sidewalk.__datasetid LIMIT 1"
        query = query.format(self.sidewalk_link_label)
        
        record = self.check_existence(query)

        return record
    

    def create_relationship(self, sidewalk_id, uuid, datasetid):

        # create relationship between the waze and its closest sidewalk node found
        id_bulkload = sidewalk_id + "-" + uuid

        waze_relationship = {"~id": id_bulkload, "~label": self.waze_node_label, 
                             "~from": sidewalk_id, "~to": uuid, 
                             "__datasetid:String(single)": datasetid}

        # store the waze relationship for bulk load
        self.waze_relationship_bulkload.append(waze_relationship)

        return
    

    def detach_waze_node(self, uuid):

        # delete waze node and link based on the uuid
        query1 = "MATCH (waze:`{}`) WHERE waze.uuid = '{}' ".format(self.waze_node_label, uuid)
        query2 = "DETACH DELETE waze"
        query = query1 + query2

        self.driver.execute_query(query)

        return
    

    def match_relationship_bulkload(self, sidewalk_id, subtype):

        # check if the relationship is in the current self.waze_relationship_bulkload with the subtype
        matched = False
        waze_found = {}

        for relation in self.waze_relationship_bulkload:

            if relation["~from"] == sidewalk_id:

                # find the waze node in self.waze_node_bulkload to find its subtype
                waze_node_id = relation["~to"]

                for node in self.waze_node_bulkload:

                    if node["~id"] == waze_node_id:

                        subtype_found = node["subtype:String(single)"]

                        if subtype_found == subtype:

                            # extract uuid and endTimeMillis of the waze node
                            waze_found["uuid"] = waze_node_id
                            waze_found["endTimeMillis"] = node["endTimeMillis:Long(single)"]

                            matched = True
                            break

                if matched:
                    break

        return matched, waze_found
    

    def create_update_waze_node_link(self, sidewalk_id, alert, time_fields, datasetid):

        # if the relationship exists where the attached waze has the same subtype, update the waze node with the one that has the lastest endtime, detach the old one
        # if the relationship doesn't exist, create the waze node and create the link
        uuid = alert["uuid"]
        subtype = alert["subtype"]
        endtime = time_fields["endTimeMillis"]

        # check if the relationship exists between sidewalk and waze nodes that has the same waze subtype as the current waze node
        query = "MATCH (sidewalk:`{}`)-[r:`{}`]->(waze:`{}`) WHERE ID(sidewalk) = '{}' AND waze.subtype = '{}' " \
            + "RETURN r, waze.uuid, waze.endTimeMillis LIMIT 1"
        query = query.format(self.sidewalk_link_label, self.waze_node_label, self.waze_node_label, sidewalk_id, subtype)

        # check if the relationship already exists or not
        record = self.check_existence(query)

        # check if the relationship exists in the current bulk load based on the subtype
        matched_bulkload, waze_found = self.match_relationship_bulkload(sidewalk_id, subtype)

        if record:

            # found the relationship on the current Neptune database
            attached_uuid = record[0]["waze.uuid"]
            attached_endtime = record[0]["waze.endTimeMillis"]

            if attached_endtime < endtime:

                print("CREATE NEW WAZE NODE AND RELATIONSHIP TO REPLACE THE OLD ONE:", uuid)

                # create the waze node and create the relationship between the sidewalk and the waze node
                self.create_waze_node(alert, time_fields, datasetid)
                self.create_relationship(sidewalk_id, uuid, datasetid)

                # detach the old waze node with the same subtype as the current waze node
                self.detach_waze_node(attached_uuid)

        elif matched_bulkload:

            # found the relationship about to be ingested by bulk load
            attached_uuid = waze_found["uuid"]
            attached_endtime = waze_found["endTimeMillis"]

            if attached_endtime < endtime:

                print("CREATE NEW WAZE NODE AND RELATIONSHIP TO REPLACE THE OLD ONE IN BULK LOAD:", uuid)

                # add the waze node and the relationship between the sidewalk and the waze node in the bulk load fields
                self.create_waze_node(alert, time_fields, datasetid)
                self.create_relationship(sidewalk_id, uuid, datasetid)

                # remove the old waze node and relationship from the bulk load fields: 
                # self.waze_node_bulkload and self.waze_relationship_bulkload
                self.waze_node_bulkload[:] = [node for node in self.waze_node_bulkload if node.get("~id") != attached_uuid]

                id_bulkload = sidewalk_id + "-" + attached_uuid
                self.waze_relationship_bulkload[:] = [relation for relation in self.waze_relationship_bulkload if relation.get("~id") != id_bulkload]

        else:

            print("CREATE WAZE NODE AND CREATE RELATIONSHIP WITH SIDEWALK:", uuid)

            # create the waze node and create the relationship between the sidewalk and the waze node
            self.create_waze_node(alert, time_fields, datasetid)
            self.create_relationship(sidewalk_id, uuid, datasetid)

        return
    

    def update_waze_endtimes_bulkload(self, uuid, endtime_ms, endtime_timestamp):

        # update endtime fields of the waze node in self.waze_node_bulkload, should be a single waze found
        for node in self.waze_node_bulkload:

            if node["~id"] == uuid:

                # update the endtime fields
                node.update({"endTimeMillis:Long(single)": endtime_ms})
                node.update({"endTime:String(single)": endtime_timestamp})

                break
        
        return
    

    def generate_post_query(self):

        # parse each alert in the waze data to add them as nodes and create links
        alerts = self.data["alerts"]
        starttime_ms = self.data["startTimeMillis"]
        endtime_ms = self.data["endTimeMillis"]
        starttime_timestamp = self.data["startTime"] # str
        endtime_timestamp = self.data["endTime"] # str
        time_fields = {"startTimeMillis": starttime_ms, "endTimeMillis": endtime_ms, 
                       "startTime": starttime_timestamp, "endTime": endtime_timestamp}
        
        # ingest or update waze alert nodes
        for alert in alerts:

            if "subtype" not in alert.keys():

                print("WARNING: THE CURRENT WAZE ALERT DOES NOT CONTAIN POSSIBLE DATA:", alert)
                continue # the alert cannot be processed without a subtype
            
            # determine if the waze node exists with uuid and label WAZE-ALERT
            uuid = alert["uuid"]
            matched = self.match_node("uuid", uuid, self.waze_node_label)
            matched_bulkload, waze_found = self.match_node_bulkload(uuid)

            if matched:

                # update endtime fields of the node on the database if it has older timestamp
                attached_endtime = matched[0]["n.endTimeMillis"]

                if attached_endtime < endtime_ms:
                    self.update_waze_endtimes(uuid, endtime_ms, endtime_timestamp)

            if matched_bulkload:
                
                # update endtime fields of the node on the bulk load if it has older timestamp
                attached_endtime = waze_found["endTimeMillis"]

                if attached_endtime < endtime_ms:
                    self.update_waze_endtimes_bulkload(uuid, endtime_ms, endtime_timestamp)
            
            if not matched and not matched_bulkload:

                # this is a new waze node, find the cloeset sidewalk GT/CE-SIDEWALK node to the new waze node
                record = self.find_sidewalk_node(alert)

                if record:
                    
                    # the sidewalk node is found
                    sidewalk_id = record[0]["ID(sidewalk)"]
                    datasetid = record[0]["sidewalk.__datasetid"]

                    # create or update waze node or link based on the current waze subtype
                    self.create_update_waze_node_link(sidewalk_id, alert, time_fields, datasetid)

                else:

                    # no GT/CE-SIDEWALK nodes are found
                    message = "WARNING: No GT/CE-SIDEWALK nodes are found, no link is created for the waze and sidewalk nodes"
                    print(message)

        return
    

    def generate_delete_query(self):

        # delete waze alert nodes and links based on their last update time
        current_time_ms = round(time.time() * 1000)
        time_limit = current_time_ms - self.waze_holdtime 
        
        # delete waze nodes and links that have last update time less than the time limit
        query1 = "MATCH (waze:`{}`) WHERE waze.endTimeMillis < {} ".format(self.waze_node_label, time_limit)
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

        return self.waze_node_bulkload, self.waze_relationship_bulkload