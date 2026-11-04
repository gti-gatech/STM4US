"""
Main script driver that contains the function lambda_handler to execute AWS Lambda function 
for Waze alerts data on API using direct ingestion approach to Neptune database.

Currently POST and DELETE requests are supported:

    POST: Save and upload Waze alerts data in json files from Waze URL endpoints to AWS S3 bucket directly,
    and ingest waze nodes and relationships to AWS Neptune database.

    Example: `curl -v -X POST 'https://<stage-url>/<stage>/api/waze-alerts/import' -d '{"id":"33.8N84.4W"}' -H 'Content-Type:application/json' -H 'Authorization:<password>'`

        Waze alerts data will be uploaded to /waze-alerts/<id>/<id>-YYYY-MM-DD-HHMMSS.json files in the 
        "its4us-upload-<stage>" S3 bucket, where <id> is the input coordinates of the polygon study area where data was retrieved.

    DELETE: Delete waze nodes and relationships that have not been updated for 15-minute period.

    Example: curl -v -X DELETE 'https://<stage-url>/<stage>/api/waze-alerts' -d '{"id":"34.0N84.4W"}' -H 'Content-Type:application/json' -H 'Authorization:<password>'

The AWS CodePipeline framework is implemented with the current repository, please go to AWS CodePipeline > Pipeline > import-waze-s3 and buildspec.yml on AWS CodeCommit for more detail.

"""

import boto3
import json
import requests
import traceback
from datetime import datetime

from params import find_params
from graph_database_driver import GraphDatabaseDriver
from query_writer_waze import WazeAlertsQueries

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

            # extract the url for the grid of interest based on the data_set_id
            datasetid = PARAMS["DATASETID"]
            url = PARAMS["WAZE_URLS"][datasetid]
            
            # get Waze alerts data from the Waze URLs in json
            waze_response = requests.get(url)
            data = waze_response.json()
            print(PARAMS["WAZE_MESSAGE"])
            
            if "alerts" not in data.keys():
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
    
            # upload the json to S3 bucket
            print(PARAMS["S3_UPLOAD_MESSAGE"])
                
            s3_response = s3_client.put_object(Body = json.dumps(data), 
                                               Bucket = PARAMS["WAZE_BUCKET"], 
                                               Key = PARAMS["WAZE_FILEPATH"])
            print(s3_response)
            print(PARAMS["S3_FINISH_MESSAGE"])

            # retrieve all sidewalk and crosswalk nodes from OSM first for attachments, their start and end nodes are also retrieved
            driverObj = GraphDatabaseDriver(PARAMS)
            sidewalk_records = driverObj.run_query(
                "CHECK", PARAMS["SIDEWALK_QUERY"], PARAMS["DATASET_QUERY_PARAMS"]
            )
            crosswalk_records = driverObj.run_query(
                "CHECK", PARAMS["CROSSWALK_QUERY"], PARAMS["DATASET_QUERY_PARAMS"]
            )
    
            # ingest or update waze alert nodes and links
            print("Parsing Waze alert nodes and links to AWS Neptune database")
            wazeObj = WazeAlertsQueries(PARAMS, data, sidewalk_records, crosswalk_records)
            wazeObj.create_transaction()
            print("Done parsing Waze alert nodes and links to AWS Neptune database")
        
        if PARAMS["METHOD"] == "DELETE":

            # delete the Waze alert nodes and links on the Neptune database based on the last updated time
            print("Removing Waze alert nodes and links on AWS Neptune database by the last updated time")
            wazeObj = WazeAlertsQueries(PARAMS, None, None, None)
            wazeObj.create_transaction()
            print("Done removing Waze alert nodes and links on AWS Neptune database")
        
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
