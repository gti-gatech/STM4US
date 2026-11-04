"""
Main script driver that computes PMD metrics based on Arcadis data files 
for an input date specified. If date is not specified, the input date is 
set to yesterday's date where available Arcadis data files exist on the 
S3 bucket for PMD calculations.
            
UPDATED: On 08/21/26, PMD are calculated daily and the result is uploaded to S3.
        
"""

import boto3
import json
import traceback
from datetime import datetime

from params import find_params
from run_compute_pmd_metrics import PMDMetricsRunDaily

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
        # compute performace metrics of the STM system and upload them to S3 in json file
        print("Computing STM performance metrics daily and uploading the metrics to S3")
        PMDMetricsRunDailyObj = PMDMetricsRunDaily(PARAMS)
        check_tracked_journey = PMDMetricsRunDailyObj.run_compute_pmd_metrics()
        print("Done computing STM performance metrics and uploading the metrics to S3")

        if "CodePipeline.job" in event.keys():

            # log codepipeline status in S3
            codepipeline_status["status"] = "COMPLETED"
            if check_tracked_journey == True:
                codepipeline_status["message"] = PARAMS["WARNING_MESSAGE"]
            else:
                codepipeline_status["message"] = PARAMS["SUCCESS_MESSAGE"]
            codepipeline_status["timestamp"] = datetime.utcnow().isoformat()

            s3_client.put_object(
                Bucket = PARAMS["ADMIN_LOG_BUCKET"],
                Key = pipeline_log_filepath,
                Body = json.dumps(codepipeline_status),
                ContentType = "application/json"
            )

            if check_tracked_journey == True:
                put_job_success(PARAMS["CODEPIPELINE_STATUS"]["jobId"], PARAMS["WARNING_MESSAGE"])
            else:
                put_job_success(PARAMS["CODEPIPELINE_STATUS"]["jobId"], PARAMS["SUCCESS_MESSAGE"])

        if check_tracked_journey == True:
            print(PARAMS["WARNING_MESSAGE"])
            status["body"] = json.dumps(PARAMS["WARNING_MESSAGE"])
        else:
            print(PARAMS["SUCCESS_MESSAGE"])
            status["body"] = json.dumps(PARAMS["SUCCESS_MESSAGE"])

        status["statusCode"] = 200
        

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
