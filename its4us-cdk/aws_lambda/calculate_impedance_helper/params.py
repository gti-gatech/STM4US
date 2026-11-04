import json
import os

def find_params(event):

    PARAMS = dict()

    TASKID = event.get("taskId")
    CODEPIPELINE_STATUS = event.get("codepipelineStatus") or dict()
    JOBID = CODEPIPELINE_STATUS.get("jobId")
    QUERY_URL = os.environ.get("QUERY_URL")
    LOADER_URL = os.environ.get("LOADER_URL")
    UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET")
    ADMIN_LOG_BUCKET = os.environ.get("ADMIN_LOG_BUCKET")

    # Export calculated impedance to PUBLIC_EXPORT_BUCKET
    S3_PUBLIC_EXPORT_BUCKET = os.environ.get("PUBLIC_EXPORT_BUCKET")

    # check if this is an independent codepipeline run
    if not JOBID:
        if "CodePipeline.job" in event.keys():
                
            # this is an independent codepipeline run
            JOB_ID = event["CodePipeline.job"]["id"]
            FUNCTION_NAME = event["CodePipeline.job"]["data"]["actionConfiguration"]["configuration"]["FunctionName"]
            JOB_DATA = event["CodePipeline.job"]["data"]["actionConfiguration"]["configuration"]["UserParameters"]
            input_params = json.loads(JOB_DATA)
        
            print("INPUTS:", input_params)
    
            PIPELINE_NAME = input_params["pipelineName"]

        else:

            JOB_ID = ""        # set CodePipeline job id and name to empty
            FUNCTION_NAME = ""
            PIPELINE_NAME = ""

        CODEPIPELINE_STATUS = {
            "jobId": JOB_ID,
            "functionName": FUNCTION_NAME,
            "pipelineName": PIPELINE_NAME,
            "status": "STATUS",
            "message": "STATUS MESSAGE.",
        }

    body = event.get("body", {})
    if isinstance(body, str):
        body = json.loads(body)
    if body is None:
        DATASETID = ""
        TEST_WAZE = False
    else:
        DATASETID = body.get("id", "")
        TEST_WAZE = body.get("testWaze", False)
    
    EXPORT_IMPEDANCE_FIELDS = {
        "~id": "~id",
        "~label": "~label",
        "~from": "~from",
        "~to": "~to",
        "Upstream Node": "Upstream Node",
        "Downstream Node": "Downstream Node",
    }

    IMPEDANCE_CALC_DATATYPES = {
        "COLUMNS": {
            "stmAdaPathLinkID": "stmAdaPathLinkID:String(single)",
            "stmAdaPathLinkLength": "stmAdaPathLinkLength:String(single)",
            "ID(na)": "~from",
            "ID(nb)": "~to"},
        "DATASETID": "__datasetid:String(single)",
        "TIMESTAMP": "Timestamp:String(single)",
    }

    IMPEDANCE_EXPORT_DATATYPES = {
        "Timestamp:String(single)": "Timestamp",
        "~from": "Upstream Node",
        "~to": "Downstream Node",
        "stmAdaPathLinkID:String(single)": "Way Id",
        "stmAdaPathLinkLength:String(single)": "Link Length",
        "None:String(single)": "None",
        "Some:String(single)": "Some",
        "Device:String(single)": "Device",
        "WChairM:String(single)": "WChairM",
        "WChairE:String(single)": "WChairE",
        "MScooter:String(single)": "MScooter",
        "LowVision:String(single)": "LowVision",
        "Blind:String(single)": "Blind",
        "Some-LowVision:String(single)": "Some-LowVision",
        "Device-LowVision:String(single)": "Device-LowVision",
        "WChairM-LowVision:String(single)": "WChairM-LowVision",
        "WChairE-LowVision:String(single)": "WChairE-LowVision",
        "MScooter-LowVision:String(single)": "MScooter-LowVision",
        "Some-Blind:String(single)": "Some-Blind",
        "Device-Blind:String(single)": "Device-Blind",
        "WChairM-Blind:String(single)": "WChairM-Blind",
        "WChairE-Blind:String(single)": "WChairE-Blind",
        "MScooter-Blind:String(single)": "MScooter-Blind"
    }

    IMPEDANCE_LINK_ATTRS = {
        "__datasetid": "__datasetid",
        "Timestamp": "Timestamp",
        "stmAdaPathLinkID": "stmAdaPathLinkID",
        "stmAdaPathLinkLength": "stmAdaPathLinkLength",
    }

    WAZE_LINK_ATTRS = {
        "__datasetid": "__datasetid",
        "__impedance_factor": "__impedance_factor",
        "__impedance_effect_type": "__impedance_effect_type",
    }

    WAZE_FIELDS = {
        "factor": "factor",
        "type": "type",
        "stmAdaPathLinkID": "stmAdaPathLinkID",
    }

    MUL = "MUL"
    ADD = "ADD"
    FACTORS_FILE_KEY = os.environ.get("FACTORS_FILE_KEY", "factors.csv")
    WAZE_TEST_FILE = "testWaze.csv"
    NUM_TRAVEL_TYPES = -18
    DATETIME_FORMAT_WITH_DASH = "%Y-%m-%d %H:%M:%S"

    OSM_WAY_LABEL = "OSM-WAY"
    IMPEDANCE_LABEL = "IMPEDANCE"
    BASE_IMPEDANCE_LABEL = "BASE-IMPEDANCE"
    WAZE_NODE_LABEL = "WAZE-ALERT"

    LAMBDA_CALC_OUTFILE = "/tmp/impedance_calculation.csv"
    S3_CALC_OUTFILE = "impedance_calculation/impedance_calculation.csv"

    LAMBDA_EXPORT_OUTFILE = "/tmp/impedance_export.csv"
    S3_EXPORT_OUTFILE = "impedance_export.csv"

    BULKLOAD_JSON = {
        "source" : f"s3://{UPLOAD_BUCKET}/impedance_calculation",
        "format" : "csv",
        "iamRoleArn" : os.environ.get("NEPTUNE_S3_ROLE_ARN"),
        "region" : os.environ.get("AWS_REGION_NAME"),
        "failOnError" : "FALSE",
        "parallelism" : "MEDIUM",
        "updateSingleCardinalityProperties" : "TRUE",
        "queueRequest" : "TRUE",
        "dependencies" : []
    }

    SUCCESS_MESSAGE = "SUCCESS: WHOLE PROCESS IS COMPLETED FOR THE REQUEST"

    STATUS = {
        "statusCode": 200,
        "body": json.dumps("STATUS MESSAGE.")
    }

    PARAMS = {
        "TASKID": TASKID,
        "CODEPIPELINE_STATUS": CODEPIPELINE_STATUS,
        "QUERY_URL": QUERY_URL,
        "LOADER_URL": LOADER_URL,
        "UPLOAD_BUCKET": UPLOAD_BUCKET,
        "ADMIN_LOG_BUCKET": ADMIN_LOG_BUCKET,
        "S3_PUBLIC_EXPORT_BUCKET": S3_PUBLIC_EXPORT_BUCKET,
        "DATASETID": DATASETID,
        "TEST_WAZE": TEST_WAZE,
        "EXPORT_IMPEDANCE_FIELDS": EXPORT_IMPEDANCE_FIELDS,
        "IMPEDANCE_CALC_DATATYPES": IMPEDANCE_CALC_DATATYPES,
        "IMPEDANCE_EXPORT_DATATYPES": IMPEDANCE_EXPORT_DATATYPES,
        "IMPEDANCE_LINK_ATTRS": IMPEDANCE_LINK_ATTRS,
        "WAZE_LINK_ATTRS": WAZE_LINK_ATTRS,
        "WAZE_FIELDS": WAZE_FIELDS,
        "MUL": MUL,
        "ADD": ADD,
        "FACTORS_FILE_KEY": FACTORS_FILE_KEY,
        "WAZE_TEST_FILE": WAZE_TEST_FILE,
        "NUM_TRAVEL_TYPES": NUM_TRAVEL_TYPES,
        "DATETIME_FORMAT_WITH_DASH": DATETIME_FORMAT_WITH_DASH,
        "OSM_WAY_LABEL": OSM_WAY_LABEL,
        "IMPEDANCE_LABEL": IMPEDANCE_LABEL,
        "BASE_IMPEDANCE_LABEL": BASE_IMPEDANCE_LABEL,
        "WAZE_NODE_LABEL": WAZE_NODE_LABEL,
        "LAMBDA_CALC_OUTFILE": LAMBDA_CALC_OUTFILE,
        "S3_CALC_OUTFILE": S3_CALC_OUTFILE,
        "LAMBDA_EXPORT_OUTFILE": LAMBDA_EXPORT_OUTFILE,
        "S3_EXPORT_OUTFILE": S3_EXPORT_OUTFILE,
        "BULKLOAD_JSON": BULKLOAD_JSON,
        "SUCCESS_MESSAGE": SUCCESS_MESSAGE,
        "STATUS": STATUS,
    }

    return PARAMS