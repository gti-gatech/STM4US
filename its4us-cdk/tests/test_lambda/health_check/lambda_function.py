import json
import boto3
import traceback

lambda_client = boto3.client("lambda")
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
    """Simple health check in CodePipeline - check existence of Lambda function"""
    
    # this is a codepipeline run
    JOB_ID = event["CodePipeline.job"]["id"]
    JOB_DATA = event["CodePipeline.job"]["data"]["actionConfiguration"]["configuration"]["UserParameters"]
    input_params = json.loads(JOB_DATA)

    print("INPUTS:", input_params)

    function_name = input_params["function_name"]
    
    try:
        # invoke the Lambda with minimal payload
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps({"Test": True})
        )
        
        status_code = response["StatusCode"]
        
        if status_code == 200:
            success_message = f"✅ Health check passed for {function_name}"
            print(success_message)

            put_job_success(JOB_ID, success_message)
            
        else:
            failed_message = f"❌ Health check failed: Status {status_code}"
            print(failed_message)

            put_job_failure(JOB_ID, failed_message)
    
    except Exception as error:

        error_string = traceback.format_exc()
        print(f"ERROR TRACEBACK:\n{error_string}")
        print(f"❌ Health check failed: {str(error)}")

        put_job_failure(JOB_ID, error_string)

    return