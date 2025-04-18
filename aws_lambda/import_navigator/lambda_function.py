"""
Main script driver that contains the function lambda_handler to execute AWS Lambda function 
for NaviGAtor scheduled and unscheduled events data on API using direct ingestion approach to Neptune database.

Currently POST request is supported:

    POST: Ingest data from CSV files located on S3 bucket to AWS Neptune database
    as nodes and edges. Events, comment and property files with latest modified datetime
    will be processed for each run from S3.

    Example: curl -v -X POST 'https://<api-id>.execute-api.<region>.amazonaws.com/<stage>/api/navigator/unscheduled/import' 
    -d '{"id":"34.0N84.4W"}' -H 'Content-Type:application/json' -H 'Authorization:<password>'


"""

import boto3
import json

from retrieve_navigator_data_s3 import RetrieveNavigatorDataS3
from preprocess import PreprocessNavigatorData
from graph_database_driver import GraphDatabaseDriver
from query_writer_navigator import NavigatorEventQueries


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
        NAVIGATOR_BUCKET = event['stageVariables']['NAVIGATOR_BUCKET']

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

            # retrieve latest modified data file for scheduled, unscheduled, comment and property data from S3
            retrieveObj = RetrieveNavigatorDataS3(NAVIGATOR_BUCKET)
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
            
            # ingest or update scheduled and unscheduled event nodes and links
            print("Parsing NaviGAtor scheduled and unscheduled event nodes and links to AWS Neptune database")
            navigatorObj = NavigatorEventQueries(QUERY_URL, method, scheduled_events, unscheduled_events, 
                                                 comments, properties, sidewalk_records, crosswalk_records, 
                                                 data_set_id)
            navigatorObj.create_transaction()
            print("Done parsing NaviGAtor events nodes and links to AWS Neptune database")
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

            # delete the Navigator data nodes and links on the Neptune database based on the last modified time
            print("Removing NaviGAtor scheduled and unscheduled event, comment, property nodes and links on AWS Neptune database by the last modified time")
            navigatorObj = NavigatorEventQueries(QUERY_URL, method, None, None, None,
                                                 None, None, None, None)
            navigatorObj.create_transaction()
            print("Done removing NaviGAtor scheduled and unscheduled event, comment, property nodes and links on AWS Neptune database")
            
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
