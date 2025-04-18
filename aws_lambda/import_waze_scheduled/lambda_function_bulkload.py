"""
Main script driver that contains the function lambda_handler to execute AWS Lambda function 
for Waze alerts data from AWS EventBridge schedule with bulk load approach.

POST:
Waze alerts data will be uploaded to <id>/<name>-YYYY-MM-DD-HHMMSS.json files in the 
S3 bucket, where id is the input to the AWS EventBridge scheduler: 
    test-waze-alerts-scheduler-1-post, 
where name is the coordinates of the polygon study area where data was retrieved.

Waze nodes and relationships are ingested to AWS Neptune database.

DELETE:
Waze nodes and associated relationships are deleted from AWS Neptune database if waze
    nodes have not been updated for more than 15 minutes.

To run the AWS Lambda function through APIs, see code in /aws_lambda/import_waze/ directory.

"""

import boto3
import json
import requests
import pandas as pd
from datetime import datetime

from query_writer_waze_bulkload import WazeAlertsQueriesBulkLoad


def lambda_handler(event, context):
    
    print("ALL EVENT FIELDS:", event)
        
    # extract data_set_id and env input parameters
    data_set_id = event["id"]
    env = event["env"] # dev or prod
    method = event["method"] # POST or DELETE

    LOADER_URL = event['stageVariables']['LOADER_URL']
    QUERY_URL = event['stageVariables']['QUERY_URL']
    LOAD_BUCKET = event['stageVariables']['LOAD_BUCKET']
    BULKLOAD_SOURCE = "s3://"+LOAD_BUCKET
    WAZE_BUCKET = event['stageVariables']['WAZE_BUCKET']

    print("DATA SET ID:", data_set_id)
    print("ENV:", env) # dev, prod
    print("METHOD USED:", method) # POST, DELETE

    if method == "POST":

        # defined the Waze endpoints where data could be retrieved; total 14 URLs
        urls = { 
            "34.0N84.4W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/f81ba212-edd3-4642-a8a3-17827f9b88d4?format=1",
            "33.8N84.4W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/00681a08-252c-46be-b256-94cbc716c3fa?format=1",
            "33.9N84.4W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/978ab657-9526-442c-bdae-78e6ee4910b0?format=1",
            "33.8N84.1W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/9a6ecef1-746f-4586-a0c8-d9d450390163?format=1",
            "34.0N84.3W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/35ac99d3-7c4a-4c36-910b-608858735923?format=1",
            "33.9N84.3W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/23a8c3f8-6ac7-4729-8c6d-7315b3ac0140?format=1",
            "33.8N84.2W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/6a708742-3e7c-4965-a2d4-98b62a94e5d9?format=1",
            "34.0N84.1W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/c3675753-18d3-4523-be4c-3185349493f0?format=1",
            "33.9N84.1W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/29219ce8-5074-4ceb-8db4-71144509dc25?format=1",
            "34.0N84.0W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/a770cf40-9b08-41bb-ae6a-5fe9427abad2?format=1",
            "34.0N84.2W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/002176a3-9b2b-4ffa-a45f-266363bb0a9f?format=1",
            "33.9N84.2W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/eb0f5ce7-a394-4cbf-9d50-28b757313fbb?format=1",
            "33.9N84.0W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/c0681289-9f5d-48c3-b4fb-3ab4de15beea?format=1",
            "33.8N84.3W": "https://www.waze.com/partnerhub-api/partners/11172875649/waze-feeds/9d31c342-3b32-417c-8498-e6b93fa15777?format=1",
        }

        # find current datetime to be used as part of the filename that will save the waze data
        now = datetime.now()
        datetime_str = now.strftime("%Y-%m-%d-%H%M%S")

        # initialize variables to store waze nodes and relationships for bulk load
        waze_nodes_bulkload = []
        waze_relationships_bulkload = []

        s3_client_bulkload = boto3.client("s3")
        config_bulkload = {
            "source" : BULKLOAD_SOURCE+"/waze/eventbridge-scheduler",
            "format" : "csv",
            "iamRoleArn" : "arn:aws:iam::760336115441:role/neptune-s3-read-access",
            "region" : "us-east-2",
            "failOnError" : "FALSE",
            "parallelism" : "MEDIUM",
            "updateSingleCardinalityProperties" : "TRUE",
            "queueRequest" : "TRUE",
            "dependencies" : []
        }

        s3_client = boto3.client("s3")
        
        # get Waze alerts data from the Waze URLs in json
        for name, url in urls.items():

            waze_response = requests.get(url)
            data = waze_response.json()

            if "alerts" not in data.keys():

                print("WARNING: NO WAZE ALERTS DATA FOUND FROM THE WAZE ENDPOINT FOR STUDY AREA:", name)
                continue

            # create the filename to save the waze data
            filename = data_set_id + "/" + name + "-" + datetime_str + ".json"

            # upload the json to S3 bucket
            print("Uploading Waze alerts data to S3 bucket:", filename)
            
            response = s3_client.put_object(Body = json.dumps(data), Bucket = WAZE_BUCKET, Key = filename)

            print("Got response:")
            print(response)
            print("Done uploading Waze alerts data to S3 bucket:", filename)

            # ingest or update waze alert nodes and links using bulk load
            print("Parsing Waze alert nodes and links to AWS Neptune database using bulk load for study area:", name)
            wazeObj = WazeAlertsQueriesBulkLoad(QUERY_URL, method, data, waze_nodes_bulkload, waze_relationships_bulkload)
            waze_nodes, waze_relations = wazeObj.create_transaction()

            waze_nodes_bulkload = waze_nodes
            waze_relationships_bulkload = waze_relations

            print("Done parsing Waze alert nodes and links to AWS Neptune database for study area:", name)
        
        # create csv files for waze node and relationship ingestions
        print("Creating CSV files for waze nodes and relationships for bulk load ingestion")
        waze_node_df = pd.DataFrame.from_dict(waze_nodes_bulkload)
        waze_relationship_df = pd.DataFrame.from_dict(waze_relationships_bulkload)

        waze_node_df.to_csv("/tmp/waze-node-scheduler.csv", encoding="utf-8", index=False)
        waze_relationship_df.to_csv("/tmp/waze-relationship-scheduler.csv", encoding="utf-8", index=False)

        # upload the csv files to s3 for the bulk load
        s3_client_bulkload.upload_file("/tmp/waze-node-scheduler.csv", LOAD_BUCKET, 
                                    "waze/eventbridge-scheduler/node.csv")
        s3_client_bulkload.upload_file("/tmp/waze-relationship-scheduler.csv", LOAD_BUCKET, 
                                    "waze/eventbridge-scheduler/relationship.csv")

        response_bulkload = requests.post(LOADER_URL, json = config_bulkload)
        print("Bulk load response:", response_bulkload.json())
        print("Whole process is completed for the request:", method)

    if method == "DELETE":

        # delete the Waze alert nodes and links on the Neptune database based on the last updated time
        print("Removing Waze alert nodes and links on AWS Neptune database by the last updated time")
        wazeObj = WazeAlertsQueriesBulkLoad(QUERY_URL, method, data = None, waze_nodes_bulkload = [], 
                                            waze_relationships_bulkload = [])
        _, _ = wazeObj.create_transaction()
        print("Done removing Waze alert nodes and links on AWS Neptune database")


    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 
                    'Access-Control-Allow-Headers': 'Content-Type', 
                    'Access-Control-Allow-Origin':'*', 
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
        'body': json.dumps("FINISHED: The request is successfully completed.")
    }

