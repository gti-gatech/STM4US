"""
Main script driver that contains the function lambda_handler to execute AWS Lambda function 
for Waze alerts data on API using direct ingestion approach to Neptune database.

Currently POST and DELETE requests are supported:

    POST: Save and upload Waze alerts data in json files from Waze URL endpoints to AWS S3 bucket directly,
    and ingest waze nodes and relationships to AWS Neptune database.

    Example: curl -v -X POST 'https://<api-id>.execute-api.<region>.amazonaws.com/<stage>/api/waze-alerts/import' -d '{"id":"34.0N84.4W"}' -H 'Content-Type:application/json' -H 'Authorization:<password>'

        Waze alerts data will be uploaded to /single-run/<id>-YYYY-MM-DD-HHMMSS.json files in the 
        S3 bucket, where <id> is the input coordinates of the polygon study area where data was retrieved.

    DELETE: Delete waze nodes and relationships that have not been updated for 15-minute period.

    Example: curl -v -X DELETE 'https://<api-id>.execute-api.<region>.amazonaws.com/<stage>/api/waze-alerts' -d '{"id":"34.0N84.4W"}' -H 'Content-Type:application/json' -H 'Authorization:<password>'

The AWS CodePipeline framework is implemented with the current repository, please go to AWS CodePipeline > Pipeline > import-waze-s3 and buildspec.yml on AWS CodeCommit for more detail.

"""

import boto3
import json
import requests
from datetime import datetime

from graph_database_driver import GraphDatabaseDriver
from query_writer_waze import WazeAlertsQueries


code_pipeline = boto3.client("codepipeline")


def put_job_success(job, message):
    """Notify CodePipeline of a successful job
    
    Args:
        job: The CodePipeline job ID
        message: A message to be logged relating to the job status
        
    Raises:
        Exception: Any exception thrown by .put_job_success_result()
    
    """
    print("Putting job is successful")
    print(message)
    code_pipeline.put_job_success_result(jobId=job)
  
  
def put_job_failure(job, message):
    """Notify CodePipeline of a failed job
    
    Args:
        job: The CodePipeline job ID
        message: A message to be logged relating to the job status
        
    Raises:
        Exception: Any exception thrown by .put_job_failure_result()
    
    """
    print("Putting job has failed")
    print(message)
    code_pipeline.put_job_failure_result(jobId=job, failureDetails={"message": message, "type": "JobFailed"})


def lambda_handler(event, context):
    
    try:
        
        print("ALL EVENT FIELDS:", event)
        
        LOADER_URL = event['stageVariables']['LOADER_URL']
        QUERY_URL = event['stageVariables']['QUERY_URL']
        WAZE_BUCKET = event['stageVariables']['WAZE_BUCKET']

        # determine if this is a CodePipeline run
        if "CodePipeline.job" in event.keys():
            
            # this is a codepipeline run
            job_id = event["CodePipeline.job"]["id"]
            job_data = event["CodePipeline.job"]["data"]["actionConfiguration"]["configuration"]["UserParameters"]
            input_params = json.loads(job_data)
        
            print("INPUTS:", input_params)
        
            env = input_params["stageVariables"]["env"]
            method = input_params["httpMethod"]
            data_set_id = str(input_params["body"]["id"])
            
        else:
        
            # find the request method: POST, DELETE and env: dev, prod for a standard API call
            env = event["stageVariables"]["env"]
            method = event["httpMethod"]
            
            # extract data_set_id input parameter
            if isinstance(event["body"], str):
                event_body = json.loads(event["body"])
                data_set_id = str(event_body["id"])
            else:
                # a dict data type here
                data_set_id = str(event["body"]["id"])

        print("ENV:", env) # dev, prod
        print("METHOD USED:", method) # POST, DELETE
        print("DATA SET ID:", data_set_id)

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

            # extract the url for the grid of interest based on the data_set_id
            url = urls[data_set_id]
            
            # get Waze alerts data from the Waze URLs in json
            waze_response = requests.get(url)
            data = waze_response.json()
            
            if "alerts" not in data.keys():
                print("WARNING: NO WAZE ALERTS DATA FOUND FROM THE WAZE ENDPOINT FOR STUDY AREA:", data_set_id)
                status = {
                    'statusCode': 400,
                    'headers': {'Content-Type': 'application/json', 
                        'Access-Control-Allow-Headers': 'Content-Type', 
                        'Access-Control-Allow-Origin':'*', 
                        'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
                    'body': json.dumps("WARNING: NO WAZE ALERTS DATA FOUND FROM THE WAZE ENDPOINT FOR STUDY AREA.")
                }
                
                if "CodePipeline.job" in event.keys():
                    put_job_success(job_id, "Job is successful!")
                
                return status

            # find current datetime to be used as part of the filename that will save the waze data
            now = datetime.now()
            datetime_str = now.strftime("%Y-%m-%d-%H%M%S")
    
            # create the S3 client
            s3_client = boto3.client("s3")
    
            # create the filename to save the waze data
            filename = "single-run/" + data_set_id + "-" + datetime_str + ".json"
    
            # upload the json to S3 bucket
            print("Uploading Waze alerts data to S3 bucket:", filename)
                
            response = s3_client.put_object(Body = json.dumps(data), Bucket = WAZE_BUCKET, Key = filename)
    
            print("Got response:")
            print(response)
            print("Done uploading Waze alerts data to S3 bucket:", filename)

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
    
            # ingest or update waze alert nodes and links
            print("Parsing Waze alert nodes and links to AWS Neptune database")
            wazeObj = WazeAlertsQueries(QUERY_URL, method, data, sidewalk_records, crosswalk_records)
            wazeObj.create_transaction()
            print("Done parsing Waze alert nodes and links to AWS Neptune database")
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

            # delete the Waze alert nodes and links on the Neptune database based on the last updated time
            print("Removing Waze alert nodes and links on AWS Neptune database by the last updated time")
            wazeObj = WazeAlertsQueries(QUERY_URL, method, None, None, None)
            wazeObj.create_transaction()
            print("Done removing Waze alert nodes and links on AWS Neptune database")
            
            status = {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 
                        'Access-Control-Allow-Headers': 'Content-Type', 
                        'Access-Control-Allow-Origin':'*', 
                        'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
                'body': json.dumps("FINISHED: The request is successfully completed.")
            }
        
        print("Whole process is now completed.")
        
        if "CodePipeline.job" in event.keys():
            put_job_success(job_id, "Job is successful!")
        
    except Exception as error:
        
        print("Function failed due to exception.") 
        print(error)
        
        if "CodePipeline.job" in event.keys():
            job_id = event["CodePipeline.job"]["id"]
            put_job_failure(job_id, "Function exception: " + str(error))
            
        status = {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 
                'Access-Control-Allow-Headers': 'Content-Type', 
                'Access-Control-Allow-Origin':'*', 
                'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
            'body': json.dumps("ERROR: LAMBDA FUNCTION FAILED TO RUN DUE TO ERROR.")
        }
        
    return status
