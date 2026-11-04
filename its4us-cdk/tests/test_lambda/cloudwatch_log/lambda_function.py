import json
import boto3
import time
import traceback

lambda_client = boto3.client("lambda")
logs_client = boto3.client("logs")
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
    """Test that Lambda writes logs to CloudWatch"""

    # this is a codepipeline run
    JOB_ID = event["CodePipeline.job"]["id"]
    JOB_DATA = event["CodePipeline.job"]["data"]["actionConfiguration"]["configuration"]["UserParameters"]
    input_params = json.loads(JOB_DATA)

    print("INPUTS:", input_params)

    function_name = input_params["function_name"]
    log_group_name = f"/aws/lambda/{function_name}"
    
    try:
        # invoke Lambda
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({'Test': True})
        )
        
        # wait for logs to appear
        time.sleep(2)
        
        # check if log group exists
        try:
            logs_client.describe_log_groups(
                logGroupNamePrefix=log_group_name
            )
            success_message = f"✅ Logs test passed - log group exists"
            print(success_message)

            put_job_success(JOB_ID, success_message)

        except logs_client.exceptions.ResourceNotFoundException:
            failed_message = f"Log group {log_group_name} not found"
            print(failed_message)

            put_job_failure(JOB_ID, failed_message)
    
    except Exception as error:

        error_string = traceback.format_exc()
        print(f"ERROR TRACEBACK:\n{error_string}")
        print(f"❌ Logs test failed: {str(error)}")

        put_job_failure(JOB_ID, error_string)

    return