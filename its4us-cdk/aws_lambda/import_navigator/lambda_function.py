"""
Main script driver that contains the function lambda_handler to execute AWS Lambda function 
for NaviGAtor scheduled and unscheduled events data on API using direct ingestion approach to Neptune database.

Currently POST request is supported:

    POST: Ingest data from CSV files located on S3 bucket its4us-navigator to AWS Neptune database
    as nodes and edges. Events, comment and property files with latest modified datetime
    will be processed for each run from S3.

    Example: curl -v -X POST 'https://<stage-url>/<stage>/api/navigator/unscheduled/import'
    -d '{"id":"34.0N84.4W"}' -H 'Content-Type:application/json' -H 'Authorization:<password>'


"""

import boto3
import json
import traceback
from datetime import datetime

from params import find_params
from retrieve_navigator_data_s3 import RetrieveNavigatorDataS3
from preprocess import PreprocessNavigatorData
from graph_database_driver import GraphDatabaseDriver
from query_writer_navigator import NavigatorEventQueries

s3_client = boto3.client("s3")
code_pipeline = boto3.client("codepipeline")


def put_job_success(job, message):
    """Notify CodePipeline of a successful job
    
    Args:
        job: The CodePipeline job ID
        message: A message to be logged relating to the job status
        
    Raises:
        Exception: Any exception thrown by .put_job_success_result()
    
    """
    print("CodePipeline: Putting job is completed")
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
    print("CodePipeline: Putting job has failed")
    print(message)
    code_pipeline.put_job_failure_result(jobId=job, 
                                         failureDetails={"message": message, "type": "JobFailed"})


def lambda_handler(event, context):

    # check if this is a health check - test function existence
    if event.get("Test") == True:
        return {"statusCode": 200, "body": "Health check OK - function exists."}

    # find necessary parameters
    PARAMS = find_params(event)

    status = PARAMS["STATUS"]
    codepipeline_status = PARAMS["CODEPIPELINE_STATUS"]
    
    pipeline_log_filepath = (
        f"CodePipeline-Logs/{PARAMS["CODEPIPELINE_STATUS"]["pipelineName"]}"
        f"/{PARAMS["CODEPIPELINE_STATUS"]["jobId"]}.json"
    )

    try:

        if PARAMS["METHOD"] == "POST":

            # retrieve latest modified data file for scheduled, unscheduled, comment and property data from S3
            retrieveObj = RetrieveNavigatorDataS3(PARAMS)
            scheduled_events = retrieveObj.retrieve_scheduled_events()
            unscheduled_events = retrieveObj.retrieve_unscheduled_events()
            comments = retrieveObj.retrieve_comments()
            properties = retrieveObj.retrieve_properties()

            if not scheduled_events and not unscheduled_events \
                and not comments and not properties:
                print(PARAMS["WARNING_MESSAGE"])

                if "CodePipeline.job" in event.keys():

                    # log codepipeline status in S3
                    codepipeline_status["status"] = "COMPLETED"
                    codepipeline_status["message"] = PARAMS["WARNING_MESSAGE"]
                    codepipeline_status["timestamp"] = datetime.utcnow().isoformat()
        
                    s3_client.put_object(
                        Bucket = PARAMS["ADMIN_LOG_BUCKET"],
                        Key = pipeline_log_filepath,
                        Body = json.dumps(codepipeline_status),
                        ContentType = "application/json"
                    )

                    put_job_success(PARAMS["CODEPIPELINE_STATUS"]["jobId"], 
                                    PARAMS["WARNING_MESSAGE"])
                
                status["statusCode"] = 200
                status["body"] = json.dumps(PARAMS["WARNING_MESSAGE"])

                return status
                
            # preprocess raw data into workable dataframe
            preprocessObj = PreprocessNavigatorData(PARAMS, scheduled_events, unscheduled_events, 
                                                    comments, properties)
            scheduled_events = preprocessObj.preprocess_scheduled_events()
            unscheduled_events = preprocessObj.preprocess_unscheduled_events()
            comments = preprocessObj.preprocess_comments()
            properties = preprocessObj.preprocess_properties()

            # retrieve all sidewalk and crosswalk nodes from OSM first for attachments, their start and end nodes are also retrieved
            driverObj = GraphDatabaseDriver(PARAMS)
            sidewalk_records = driverObj.run_query("CHECK", PARAMS["SIDEWALK_QUERY"])
            crosswalk_records = driverObj.run_query("CHECK", PARAMS["CROSSWALK_QUERY"])
            
            # ingest or update scheduled and unscheduled event nodes and links
            print("Parsing NaviGAtor scheduled and unscheduled event nodes and links to AWS Neptune database")
            navigatorObj = NavigatorEventQueries(PARAMS, scheduled_events, unscheduled_events, 
                                                 comments, properties, sidewalk_records, crosswalk_records)
            navigatorObj.create_transaction()
            print("Done parsing NaviGAtor events nodes and links to AWS Neptune database")

        if PARAMS["METHOD"] == "DELETE":

            # delete the Navigator data nodes and links on the Neptune database based on the last modified time
            message = "Removing NaviGAtor scheduled and unscheduled event, comment, " \
                + "property nodes and links on AWS Neptune database by the last modified time"
            print(message)
            navigatorObj = NavigatorEventQueries(PARAMS, None, None, None,
                                                 None, None, None)
            navigatorObj.create_transaction()
            message = "Done removing NaviGAtor scheduled and unscheduled event, comment, " \
                + "property nodes and links on AWS Neptune database"
            print(message)
        
        if "CodePipeline.job" in event.keys():

            # log codepipeline status in S3
            codepipeline_status["status"] = "COMPLETED"
            codepipeline_status["message"] = PARAMS["SUCCESS_MESSAGE"]
            codepipeline_status["timestamp"] = datetime.utcnow().isoformat()

            s3_client.put_object(
                Bucket = PARAMS["ADMIN_LOG_BUCKET"],
                Key = pipeline_log_filepath,
                Body = json.dumps(codepipeline_status),
                ContentType = "application/json"
            )

            put_job_success(PARAMS["CODEPIPELINE_STATUS"]["jobId"], 
                            PARAMS["SUCCESS_MESSAGE"])
        
        print(PARAMS["SUCCESS_MESSAGE"])
        status["statusCode"] = 200
        status["body"] = json.dumps(PARAMS["SUCCESS_MESSAGE"])

    except Exception as error:
        
        print("ERROR: Function failed due to exception.") 
        error_string = traceback.format_exc()
        print(f"ERROR TRACEBACK:\n{error_string}")
        
        if "CodePipeline.job" in event.keys():

            # log codepipeline status in S3
            codepipeline_status["status"] = "FAILED"
            codepipeline_status["message"] = "Function exception: " + error_string
            codepipeline_status["timestamp"] = datetime.utcnow().isoformat()

            s3_client.put_object(
                Bucket = PARAMS["ADMIN_LOG_BUCKET"],
                Key = pipeline_log_filepath,
                Body = json.dumps(codepipeline_status),
                ContentType = "application/json"
            )

            put_job_failure(PARAMS["CODEPIPELINE_STATUS"]["jobId"], 
                            "Function exception: " + error_string)
            
        status["statusCode"] = 500
        status["body"] = json.dumps(error_string)
        
    return status
