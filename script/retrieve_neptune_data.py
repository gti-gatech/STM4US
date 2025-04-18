"""
The script downloads data from AWS Neptune database and stores the data to CSV files.
Currently Waze and NaviGAtor data are implemented in the script.

"""


import csv
from neo4j import GraphDatabase, RoutingControl


def retrieve_waze(driver):
    records, _, _ = driver.execute_query(
        "MATCH (waze:`WAZE-ALERT`) RETURN waze",
        routing_=RoutingControl.READ)
    
    waze_data = []
    for record in records:

        # print(record.data()["waze"]) # dict
        waze_data.append(record.data()["waze"])

    return waze_data

def retrieve_waze_relationships(driver):

    records, _, _ = driver.execute_query(
        "MATCH (osm:`OSM-WAY`)-[r:`WAZE-ALERT`]->(waze:`WAZE-ALERT`) "
        "RETURN ID(osm), r.__datasetid as __datasetid, r.__impedance_factor as __impedance_factor, "
        "r.__impedance_effect_type as __impedance_effect_type, waze.uuid, ID(waze)",
        routing_=RoutingControl.READ)
    
    waze_relations = []
    for record in records:

        data = {"ID(osm)": record.data()["ID(osm)"], "__datasetid": record.data()["__datasetid"], 
                "__impedance_factor": record.data()["__impedance_factor"],
                "__impedance_effect_type": record.data()["__impedance_effect_type"], 
                "waze.uuid": record.data()["waze.uuid"],
                "ID(waze)": record.data()["ID(waze)"]}
        
        waze_relations.append(data)

    return waze_relations


def retrieve_navigator_events(driver):
    records, _, _ = driver.execute_query(
        "MATCH (event:`NAVIGATOR-EVENT`) RETURN event",
        routing_=RoutingControl.READ)
    
    navigator_data = []
    for record in records:

        # print(record.data()["event"]) # dict
        navigator_data.append(record.data()["event"])

    return navigator_data


def retrieve_navigator_comment(driver):

    records, _, _ = driver.execute_query(
        "MATCH (event:`NAVIGATOR-EVENT`)-[r:`NAVIGATOR-EVENT`]->(comment:`NAVIGATOR-EVENT-COMMENT`) "
        "RETURN event.event_id, comment",
        routing_=RoutingControl.READ)
    
    event_comment_data = []
    for record in records:

        # print(record.data()["comment"]) # dict
        event_id = {"event.event_id": record.data()["event.event_id"]}
        comment = record.data()["comment"]
        event_comment = {**event_id, **comment}
        event_comment_data.append(event_comment)

    return event_comment_data


def retrieve_navigator_property(driver):

    records, _, _ = driver.execute_query(
        "MATCH (event:`NAVIGATOR-EVENT`)-[r:`NAVIGATOR-EVENT`]->(property1:`NAVIGATOR-EVENT-PROPERTY`) "
        "RETURN event.event_id, property1",
        routing_=RoutingControl.READ)
    
    event_property_data = []
    for record in records:

        # print(record.data()["property1"]) # dict
        event_id = {"event.event_id": record.data()["event.event_id"]}
        property1 = record.data()["property1"]
        event_property = {**event_id, **property1}
        event_property_data.append(event_property)

    return event_property_data


def retrieve_navigator_relationships(driver):

    records, _, _ = driver.execute_query(
        "MATCH (osm:`OSM-WAY`)-[r:`NAVIGATOR-EVENT`]->(event:`NAVIGATOR-EVENT`) "
        "RETURN ID(osm), r.__datasetid as __datasetid, r.__impedance_factor as __impedance_factor, "
        "r.__impedance_effect_type as __impedance_effect_type, event.event_id, ID(event)",
        routing_=RoutingControl.READ)
    
    navigator_relations = []
    for record in records:

        data = {"ID(osm)": record.data()["ID(osm)"], "__datasetid": record.data()["__datasetid"], 
                "__impedance_factor": record.data()["__impedance_factor"],
                "__impedance_effect_type": record.data()["__impedance_effect_type"], 
                "event.event_id": record.data()["event.event_id"],
                "ID(event)": record.data()["ID(event)"]}
        
        navigator_relations.append(data)

    return navigator_relations


def retrieve_waze_all(URI, AUTH, node_filename, relation_filename):

    with GraphDatabase.driver(URI, auth=AUTH, encrypted=True) as driver:
    
        # retrieve all waze data from Neptune database prod or dev and save to CSV files
        waze_data = retrieve_waze(driver)
        headers = ["country", "startTimeMillis", "city", "location-x", "reportRating", "location-y",
                   "reportByMunicipalityUser", "confidence", "type", "uuid", "roadType", "endTimeMillis",
                   "magvar", "nThumbsUp", "subtype", "street", "reportDescription", "__datasetid", 
                   "reliability", "startTime", "endTime", "pubMillis", "speed", "wazeData", "inscale", 
                   "reportMood", "id", "reportBy", "additionalInfo", "nearBy"]
        
        with open(node_filename, "w") as csvfile: 
            writer = csv.DictWriter(csvfile, fieldnames = headers) 
            writer.writeheader() 
            writer.writerows(waze_data) 
        
        waze_relations = retrieve_waze_relationships(driver)
        headers = ["ID(osm)", "__datasetid", "__impedance_factor", "__impedance_effect_type", "waze.uuid", "ID(waze)"]

        with open(relation_filename, "w") as csvfile: 
            writer = csv.DictWriter(csvfile, fieldnames = headers) 
            writer.writeheader() 
            writer.writerows(waze_relations) 

    print("Done saving waze data to CSV files!")

    return


def retrieve_navigator_all(URI, AUTH, node_filename, relation_filename, event_comment_filename,
                           event_property_filename):
    
    with GraphDatabase.driver(URI, auth=AUTH, encrypted=True) as driver:

        # retrieve all navigator data from Neptune database prod or dev and save to CSV files
        print("Saving NaviGAtor Event Nodes to CSV")
        navigator_event_data = retrieve_navigator_events(driver)
        headers = ["end_longitude", "county", "end_primary_road", "type", "end_cross_road", "response_plan_id",
                "mile_marker", "road_type", "actual_end_time", "actual_start_time", "state", "end_city",
                "end_road_type", "longitude", "end_length", "cross_road", "end_direction", "end_county",
                "primary_road", "modified_date", "lane_pattern", "version", "created_by", "start_time",
                "district", "end_angle", "__datasetid", "status", "city", "latitude", "end_orientation",
                "external_id", "end_latitude", "event_update", "subtype", "angle", "end_district",
                "direction", "severity", "orientation", "agency", "length", "end_time", "lane_blocked",
                "end_mile_marker", "__modified_date_ms", "event_id", "modified_by", "end_state", "created_date",
                "detection_method"]

        with open(node_filename, "w") as csvfile: 
            writer = csv.DictWriter(csvfile, fieldnames = headers) 
            writer.writeheader() 
            writer.writerows(navigator_event_data) 
        

        # save navigator_event_relations to csv file
        print("Saving NaviGAtor Event OSM Relationship to CSV")
        navigator_event_relations = retrieve_navigator_relationships(driver)
        headers = ["ID(osm)", "__datasetid", "__impedance_factor", "__impedance_effect_type", "event.event_id", "ID(event)"]

        with open(relation_filename, "w") as csvfile: 
            writer = csv.DictWriter(csvfile, fieldnames = headers) 
            writer.writeheader() 
            writer.writerows(navigator_event_relations) 

        
        # save navigator_comment_data to csv file
        print("Saving NaviGAtor Comment Nodes to CSV")
        navigator_comment_data = retrieve_navigator_comment(driver)
        headers = ["event.event_id", "event_id", "comments", "agency", "added_by", "added_date", "__datasetid", "comment_id"]

        with open(event_comment_filename, "w") as csvfile: 
            writer = csv.DictWriter(csvfile, fieldnames = headers) 
            writer.writeheader() 
            writer.writerows(navigator_comment_data) 

        
        # save navigator_property_data to csv file
        print("Saving NaviGAtor Property Nodes to CSV")
        navigator_property_data = retrieve_navigator_property(driver)
        headers = ["event.event_id", "event_id", "name", "__datasetid", "id", "type", "version", "value", "property_id", "misc"]

        with open(event_property_filename, "w") as csvfile: 
            writer = csv.DictWriter(csvfile, fieldnames = headers) 
            writer.writeheader() 
            writer.writerows(navigator_property_data) 

    print("Done saving NaviGAtor data to CSV files!")
    
    return
    

if __name__ == "__main__":

    env = "dev"
    data_type = "navigator" # "waze", "navigator"

    # specify CSV filenames to save the waze data
    waze_node_filename = "waze_nodes_samples_neptune_20240216.csv" # filename for waze nodes
    waze_relation_filename = "waze_relationship_samples_neptune_20240216.csv" # filename for waze relationships

    # specify CSV filenames to save the navigator data
    event_node_filename = "navigator_event_nodes_samples_neptune_20240216.csv" # filename for navigator nodes
    event_relation_filename = "navigator_event_relationship_samples_neptune_20240216.csv" # filename for navigator relationships
    event_comment_filename = "navigator_event_comment_relationship_samples_neptune_20240216.csv" # filename for event-comment relationships
    event_property_filename = "navigator_event_property_relationship_samples_neptune_20240216.csv" # filename for event-property relationships

    URI = "bolt://<neptune-url>.neptune.amazonaws.com:8182"
    AUTH = ("username", "password") # not used

    if data_type == "waze":

        # retrieve waze data and save to CSV files
        retrieve_waze_all(URI, AUTH, waze_node_filename, waze_relation_filename)

    else:

        # retrieve navigator data and save to CSV files
        retrieve_navigator_all(URI, AUTH, event_node_filename, event_relation_filename,
                               event_comment_filename, event_property_filename)

