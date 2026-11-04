import os
import json

def find_params(event):

    PARAMS = dict()

    # determine if this is a CodePipeline run
    if "CodePipeline.job" in event.keys():
        
        # this is a codepipeline run
        JOB_ID = event["CodePipeline.job"]["id"]
        FUNCTION_NAME = event["CodePipeline.job"]["data"]["actionConfiguration"]["configuration"]["FunctionName"]
        JOB_DATA = event["CodePipeline.job"]["data"]["actionConfiguration"]["configuration"]["UserParameters"]
        input_params = json.loads(JOB_DATA)
    
        print("INPUTS:", input_params)

        PIPELINE_NAME = input_params["pipelineName"]
    
        ADMIN_LOG_BUCKET = input_params["stageVariables"]["ADMIN_LOG_BUCKET"]
        LAMBDA_HELPER = input_params["stageVariables"]["BASE_IMPEDANCE_CALCULATION_HELPER_ARN"]

    else:

        JOB_ID = ""        # set CodePipeline job id and name to empty
        FUNCTION_NAME = ""
        PIPELINE_NAME = ""

        ADMIN_LOG_BUCKET = os.environ.get("ADMIN_LOG_BUCKET")
        LAMBDA_HELPER = os.environ.get("BASE_IMPEDANCE_CALCULATION_HELPER_ARN")

    SUCCESS_MESSAGE = "SUCCESS: TASK INITIATED SUCCESSFULLY" 

    STATUS_BODY = {
        "status": "PENDING",
        "message": "STATUS MESSAGE.",
    }

    CODEPIPELINE_STATUS = {
        "jobId": JOB_ID,
        "functionName": FUNCTION_NAME,
        "pipelineName": PIPELINE_NAME,
        "status": "STATUS",
        "message": "STATUS MESSAGE.",
    }

    PARAMS = {
        "ADMIN_LOG_BUCKET": ADMIN_LOG_BUCKET,
        "LAMBDA_HELPER": LAMBDA_HELPER,
        "SUCCESS_MESSAGE": SUCCESS_MESSAGE,
        "STATUS_BODY": STATUS_BODY,
        "CODEPIPELINE_STATUS": CODEPIPELINE_STATUS,
    }

    return PARAMS