import json
import boto3
import traceback
import logging
import pandas as pd
from datetime import datetime

from params import find_params
from query_sidewalks import query_sidewalks
from calculate_impedance import calculate_impedance
from generate_impedance_output import generate_impedance_output

s3_client = boto3.client("s3")
code_pipeline = boto3.client("codepipeline")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def put_job_success(job, message):
    """Notify CodePipeline of a successful job
    
    Args:
        job: The CodePipeline job ID
        message: A message to be logged relating to the job status
        
    Raises:
        Exception: Any exception thrown by .put_job_success_result()
    
    """
    logger.info("CodePipeline: Putting job is completed")
    logger.info(message)

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
    logger.error("CodePipeline: Putting job has failed")
    logger.error(message)

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
        # Update status to IN_PROGRESS
        update_task_status(PARAMS, "IN_PROGRESS")
        
        # Do your long-running work here
        result = perform_long_running_task(PARAMS)
        
        # Update with success result
        update_task_status(PARAMS, "COMPLETED", result = result)

        if PARAMS["CODEPIPELINE_STATUS"]["jobId"]:

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
        
    except Exception as e:
        # Update with error
        update_task_status(PARAMS, "FAILED", error_message = str(e))

        logger.error("Function failed due to exception")
        error_string = traceback.format_exc()
        logger.error(f"ERROR TRACEBACK:\n{error_string}")
        
        if PARAMS["CODEPIPELINE_STATUS"]["jobId"]:

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


def update_task_status(PARAMS, status, result = None, error_message = None):
    
    # Get current status file
    try:
        response = s3_client.get_object(
            Bucket = PARAMS["ADMIN_LOG_BUCKET"],
            Key = f"{PARAMS["TASKID"]}.json"
        )
        status_data = json.loads(response["Body"].read().decode("utf-8"))
    
    except s3_client.exceptions.NoSuchKey:
        status_data = {"taskId": PARAMS["TASKID"]}
    
    # Update fields
    status_data["status"] = status
    status_data["updatedAt"] = datetime.utcnow().isoformat()
    
    if result is not None:
        status_data["result"] = result
    
    if error_message is not None:
        status_data["errorMessage"] = error_message
    
    # Write back to S3
    s3_client.put_object(
        Bucket = PARAMS["ADMIN_LOG_BUCKET"],
        Key = f"{PARAMS["TASKID"]}.json",
        Body = json.dumps(status_data),
        ContentType = "application/json"
    )

def perform_long_running_task(PARAMS):

    # query sidewalks data
    sidewalks, defects, ramps, curbs, curbCuts, crossings, busStops = query_sidewalks(PARAMS)

    # Download factors.csv from S3
    factors_file_local = "/tmp/factors.csv"
    s3_client.download_file(
        PARAMS["S3_UPLOAD_BUCKET"],
        PARAMS["FACTORS_FILE_KEY"],
        factors_file_local
    )
    
    # load factors.csv file
    factors = pd.read_csv(factors_file_local, na_values = "NA")

    travelTypes = factors.columns.tolist()[PARAMS["NUM_TRAVEL_TYPES"]:]
    speeds = factors[travelTypes].iloc[0]
    currentTime = datetime.now().strftime(PARAMS["DATETIME_FORMAT_WITH_DASH"])

    # calculate impedance 
    impedance, calc_errors = calculate_impedance(PARAMS, factors, travelTypes, speeds, sidewalks, defects, 
                                    ramps, curbs, curbCuts, crossings, busStops)

    # Log any errors that occurred during calculation
    if calc_errors:
        error_summary = f"Encountered {len(calc_errors)} error(s) during base impedance calculation"
        logger.warning(error_summary)
        for error in calc_errors:
            logger.warning(f"  - {error['type']}: {error['message']}")

    # generate impedance output
    if not impedance.empty:
        bulkLoad_response = generate_impedance_output(PARAMS, travelTypes, currentTime, impedance)
        output = {
            "bulkLoadResponse": bulkLoad_response.json(),
            "warnings": calc_errors if calc_errors else []
        }
    else:
        output = {
            "bulkLoadResponse": None,
            "warning": "SIDEWALK DATA EMPTY, NO IMPEDANCE DATA CALCULATED.",
            "warnings": calc_errors if calc_errors else []
        }
        logger.warning(output["warning"])
    
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", 
                    "Access-Control-Allow-Headers": "Content-Type", 
                    "Access-Control-Allow-Origin":"*", 
                    "Access-Control-Allow-Methods": "OPTIONS,POST,DELETE"},
        "body": json.dumps(output)
    }

    