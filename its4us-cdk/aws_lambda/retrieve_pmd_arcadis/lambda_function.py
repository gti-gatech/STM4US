"""
Main script driver that retrieves PMD data files from Arcadis 
for an input date specified. If date is not specified, the input date is 
set to yesterday's date where available Arcadis data files could be 
retrieved and uploaded to S3 bucket.

"""

import json
import boto3
import traceback
from datetime import datetime

from params import find_params
from retrieve_pmd_data_arcadis import RetrievePMDArcadis

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

        # retrieve and upload PMD data files from Arcadis to S3 for the date specified
        print("Retrieving and uploading data files from Arcadis to S3")
        RetrievePMDArcadisObj = RetrievePMDArcadis(PARAMS)
        RetrievePMDArcadisObj.retrieve_upload_data_arcadis()

        message = "Done retrieving and uploading data files from Arcadis to S3: " \
            + PARAMS["S3_UPLOAD_BUCKET"] + "/" + PARAMS["S3_UPLOAD_FOLDER"]
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