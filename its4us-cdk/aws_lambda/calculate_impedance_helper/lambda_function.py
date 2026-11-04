import json
import boto3
import traceback
import pandas as pd
from datetime import datetime
from time import time

from params import find_params
from query_impedance_waze import query_database
from calculate_impedance import calculate_impedance
from generate_impedance_output import generate_impedance_output

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
    
    #task_id = event.get("taskId")
    task_id = PARAMS["TASKID"]

    status = PARAMS["STATUS"]
    codepipeline_status = PARAMS["CODEPIPELINE_STATUS"]

    pipeline_log_filepath = (
        f"CodePipeline-Logs/{PARAMS["CODEPIPELINE_STATUS"]["pipelineName"]}"
        f"/{PARAMS["CODEPIPELINE_STATUS"]["jobId"]}.json"
    )
    
    try:
        # Update status to IN_PROGRESS
        if task_id:
            status_data = {
                "taskId": task_id,
                "status": "IN_PROGRESS",
                "updatedAt": datetime.utcnow().isoformat()
            }
            s3_client.put_object(
                Bucket=PARAMS["ADMIN_LOG_BUCKET"],
                Key=f"{task_id}.json",
                Body=json.dumps(status_data),
                ContentType="application/json"
            )

        print("Start reading csv:", time())

        # Download factors.csv from S3 upload bucket
        factors_file_local = "/tmp/factors.csv"
        s3_client.download_file(
            PARAMS["UPLOAD_BUCKET"],
            PARAMS["FACTORS_FILE_KEY"],
            factors_file_local
        )
        
        # read factors.csv
        factors = pd.read_csv(factors_file_local, na_values = "NA")
        travelTypes = factors.columns.tolist()[PARAMS["NUM_TRAVEL_TYPES"]:]
        currentTime = datetime.now().strftime(PARAMS["DATETIME_FORMAT_WITH_DASH"])

        # query database on impedance and waze
        baseImpedance, waze_df = query_database(PARAMS, travelTypes)

        if baseImpedance:
            # calculate impedance    
            impedance, calc_errors = calculate_impedance(PARAMS, travelTypes, baseImpedance, waze_df)

            # Log any errors that occurred during calculation
            if calc_errors:
                error_summary = f"Encountered {len(calc_errors)} error(s) during Waze impedance calculation"
                print(error_summary)
                for error in calc_errors:
                    print(f"  - {error['type']}: {error['message']}")

            # upload output to database and s3
            if not impedance.empty:
                bulkLoad_response = generate_impedance_output(PARAMS, travelTypes, currentTime, impedance)
                output = {
                    "bulkLoadResponse": bulkLoad_response.json(),
                    "warnings": calc_errors if calc_errors else []
                }
            else:
                output = {
                    "bulkLoadResponse": None,
                    "warning": "IMPEDANCE DATA EMPTY AFTER CALCULATION, NO OUTPUT GENERATED.",
                    "warnings": calc_errors if calc_errors else []
                }
                print(output["warning"])
        else:
            output = {
                "bulkLoadResponse": None,
                "warning": "BASE IMPEDANCE DATA EMPTY, NO IMPEDANCE DATA CALCULATED.",
                "warnings": []
            }
            print(output["warning"])

        # Update status to COMPLETED
        if task_id:
            status_data = {
                "taskId": task_id,
                "status": "COMPLETED",
                "updatedAt": datetime.utcnow().isoformat(),
                "result": output
            }
            s3_client.put_object(
                Bucket=PARAMS["ADMIN_LOG_BUCKET"],
                Key=f"{task_id}.json",
                Body=json.dumps(status_data),
                ContentType="application/json"
            )

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

        # Update status to FAILED
        if task_id:
            status_data = {
                "taskId": task_id,
                "status": "FAILED",
                "updatedAt": datetime.utcnow().isoformat(),
                "error": str(e)
            }
            s3_client.put_object(
                Bucket=PARAMS["ADMIN_LOG_BUCKET"],
                Key=f"{task_id}.json",
                Body=json.dumps(status_data),
                ContentType="application/json"
            )

        print("ERROR: Function failed due to exception.") 
        error_string = traceback.format_exc()
        print(f"ERROR TRACEBACK:\n{error_string}")

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

        #raise

    return status

    