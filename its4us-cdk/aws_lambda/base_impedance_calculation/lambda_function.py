import json
import boto3
import uuid
import traceback
from datetime import datetime

from params import find_params

lambda_client = boto3.client("lambda")
s3_client = boto3.client("s3")
code_pipeline = boto3.client("codepipeline")


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

    status = dict()
    status_body = PARAMS["STATUS_BODY"]
    codepipeline_status = PARAMS["CODEPIPELINE_STATUS"]

    pipeline_log_filepath = (
        f"CodePipeline-Logs/{PARAMS["CODEPIPELINE_STATUS"]["pipelineName"]}"
        f"/{PARAMS["CODEPIPELINE_STATUS"]["jobId"]}.json"
    )

    # Generate unique task ID
    task_id = str(uuid.uuid4())

    status_body["taskId"] = task_id
    
    # Store initial status in S3
    status_data = {
        "taskId": task_id,
        "status": "PENDING",
        "createdAt": datetime.utcnow().isoformat(),
        "updatedAt": datetime.utcnow().isoformat()
    }
    
    s3_client.put_object(
        Bucket = PARAMS["ADMIN_LOG_BUCKET"],
        Key = f"{task_id}.json",
        Body = json.dumps(status_data),
        ContentType = "application/json"
    )
    
    # Invoke worker Lambda asynchronously
    event["taskId"] = task_id
    event["codepipelineStatus"] = PARAMS["CODEPIPELINE_STATUS"] # codepipeline data
    
    try:
        # invoke the Lambda function helper: base-impedance-calculation-helper
        lambda_client.invoke(
            FunctionName = PARAMS["LAMBDA_HELPER"],
            InvocationType = "Event",
            Payload = json.dumps(event)
        )

        status["statusCode"] = 202
        status_body["message"] = PARAMS["SUCCESS_MESSAGE"]

    except Exception as error:

        print("ERROR: Function failed to invoke due to exception.") 
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
        status_body["message"] = json.dumps(error_string)

    status["body"] = json.dumps(status_body)

    
    return status