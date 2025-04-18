"""
Main script driver that contains the function lambda_handler to execute AWS Lambda function 
for NaviGAtor scheduled and unscheduled events data from AWS EventBridge schedule where direct ingestions 
are done on the Neptune database.

Currently POST request is supported:

    POST: Ingest data from CSV files located on S3 bucket to AWS Neptune database
    as nodes and edges. Events, comment and property files with latest modified datetime
    will be processed for each run from S3.

    Example: curl -v -X POST 'https://<api-id>.execute-api.<region>.amazonaws.com/<stage>/api/navigator/unscheduled/import' 
    -d '{"id":"34.0N84.4W"}' -H 'Content-Type:application/json' -H 'Authorization:<password>'


"""

import json

from retrieve_navigator_data_s3 import RetrieveNavigatorDataS3
from preprocess import PreprocessNavigatorData
from graph_database_driver import GraphDatabaseDriver
from query_writer_navigator import NavigatorEventQueries


def lambda_handler(event, context):
    
    print("ALL EVENT FIELDS:", event)

    # extract data_set_id and env input parameters
    data_set_id = event["id"]
    env = event["env"] # dev or prod
    method = event["method"] # POST or DELETE

    print("DATA SET ID:", data_set_id)
    print("ENV:", env) # dev, prod
    print("METHOD USED:", method) # POST, DELETE

    LOADER_URL = event['stageVariables']['LOADER_URL']
    QUERY_URL = event['stageVariables']['QUERY_URL']

    if method == "POST":

        # retrieve latest modified data file for scheduled, unscheduled, comment and property data from S3
        retrieveObj = RetrieveNavigatorDataS3()
        scheduled_events = retrieveObj.retrieve_scheduled_events()
        unscheduled_events = retrieveObj.retrieve_unscheduled_events()
        comments = retrieveObj.retrieve_comments()
        properties = retrieveObj.retrieve_properties()
        
        # preprocess raw data into workable dataframe
        preprocessObj = PreprocessNavigatorData(scheduled_events, unscheduled_events, comments, properties)
        scheduled_events = preprocessObj.preprocess_scheduled_events()
        unscheduled_events = preprocessObj.preprocess_unscheduled_events()
        comments = preprocessObj.preprocess_comments()
        properties = preprocessObj.preprocess_properties()

        # retrieve all sidewalk and crosswalk nodes from OSM first for attachments, their start and end nodes are also retrieved
        sidewalk_query1 = "MATCH (node1:`OSM-NODE`)-[:FIRST]-(sidewalk:`OSM-WAY` {{footway: 'sidewalk', __datasetid: '{}'}})-"
        sidewalk_query2 = "[:LAST]-(node2:`OSM-NODE`) RETURN sidewalk, node1, node2"
        sidewalk_query = sidewalk_query1 + sidewalk_query2
        sidewalk_query = sidewalk_query.format(data_set_id)

        crosswalk_query1 = "MATCH (node1:`OSM-NODE`)-[:FIRST]-(crosswalk:`OSM-WAY` {{footway: 'crossing', __datasetid: '{}'}})-"
        crosswalk_query2 = "[:LAST]-(node2:`OSM-NODE`) RETURN crosswalk, node1, node2"
        crosswalk_query = crosswalk_query1 + crosswalk_query2
        crosswalk_query = crosswalk_query.format(data_set_id)
        
        driverObj = GraphDatabaseDriver(QUERY_URL)
        sidewalk_records = driverObj.run_query("CHECK", sidewalk_query)
        crosswalk_records = driverObj.run_query("CHECK", crosswalk_query)
        
        # ingest or update scheduled and unscheduled events nodes and links
        print("Parsing NaviGAtor scheduled and unscheduled events nodes and links to AWS Neptune database")
        navigatorObj = NavigatorEventQueries(QUERY_URL, method, scheduled_events, unscheduled_events, 
                                                   comments, properties, sidewalk_records, crosswalk_records, 
                                                   data_set_id)
        navigatorObj.create_transaction()
        print("Done parsing NaviGAtor unscheduled events nodes and links to AWS Neptune database")
        print("Whole process is completed for the request:", method)

        status = {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json', 
                    'Access-Control-Allow-Headers': 'Content-Type', 
                    'Access-Control-Allow-Origin':'*', 
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
            'body': json.dumps("FINISHED: The request is successfully completed.")
        }
 

    if method == "DELETE":
        
        # extract data_set_id input parameter
        data_set_id = event["id"] # currently not used

        print("DATA SET ID:", data_set_id)

        # delete the Navigator data nodes and links on the Neptune database based on the last modified time
        print("Removing NaviGAtor scheduled and unscheduled event, comment, property nodes and links on AWS Neptune database by the last modified time")
        wazeObj = NavigatorEventQueries(QUERY_URL, method, None, None, None,
                                              None, None, None, None)
        wazeObj.create_transaction()
        print("Done removing NaviGAtor scheduled and unscheduled event, comment, property nodes and links on AWS Neptune database")
        
        status = {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json', 
                    'Access-Control-Allow-Headers': 'Content-Type', 
                    'Access-Control-Allow-Origin':'*', 
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
            'body': json.dumps("FINISHED: The request is successfully completed.")
        }
        

    return status
