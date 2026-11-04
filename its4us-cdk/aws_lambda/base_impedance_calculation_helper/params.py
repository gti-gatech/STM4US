import json
import os
import logging

logger = logging.getLogger(__name__)


def find_params(event):

    PARAMS = dict()

    # PARAMETERS
    logger.debug(f"Event fields: {event}")

    TASKID = event.get("taskId")
    CODEPIPELINE_STATUS = event.get("codepipelineStatus") or dict()
    JOBID = CODEPIPELINE_STATUS.get("jobId")
    ENV = os.environ.get("STAGE")
    QUERY_URL = os.environ.get("QUERY_URL")
    LOADER_URL = os.environ.get("LOADER_URL")
    S3_UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET")
    ADMIN_LOG_BUCKET = os.environ.get("ADMIN_LOG_BUCKET")

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
    else:
        DATASETID = body.get("id", "")

    NUM_TRAVEL_TYPES = -18
    FACTORS_FILE_KEY = os.environ.get("FACTORS_FILE_KEY", "factors.csv")
    MUL = "MUL"
    ADD = "ADD"
    DATETIME_FORMAT_WITH_DASH = "%Y-%m-%d %H:%M:%S"

    SIDEWALK_LABEL = "GT/CE-SIDEWALK"
    DEFECT_LABEL = "GT/CE-SIDEWALK-DEFECT"
    RAMP_LABEL = "GT/CE-SIDEWALK-RAMP"
    CURB_LABEL = "GT/CE-SIDEWALK-CURB"
    CURB_CUT_LABEL = "GT/CE-SIDEWALK-CURB-CUT"
    CROSSING_LABEL = "GT/CE-SIDEWALK-CROSSING"
    BUS_STOP_LABEL = "GT/CE-SIDEWALK-BUS-STOP"

    BASE_IMPEDANCE_LABEL = "BASE-IMPEDANCE"

    EXPORT_IMPEDANCE_FIELDS = {
        "~id": "~id",
        "~label": "~label",
        "~from": "~from",
        "~to": "~to",
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

    SIDEWALK_LINK_ATTRS = {
        "ID(na)": "ID(na)",
        "ID(nb)": "ID(nb)",
        "__datasetid": "__datasetid",
        "Timestamp": "Timestamp",
        "stmAdaPathLinkID": "stmAdaPathLinkID",
        "stmAdaPathLinkLength": "stmAdaPathLinkLength",
        "stmAssetDefectReportPedLinkID": "stmAssetDefectReportPedLinkID",
    }

    FACTORS_FIELDS = {
        "Impedance Effect Type": "Impedance Effect Type",
        "Units": "Units",
        "Enumeration": "Enumeration",
        "Enumerated": "Enumerated",
        "Variable Name": "Variable Name",
        "Lower Constraint Bound": "Lower Constraint Bound",
        "Upper Constraint Bound": "Upper Constraint Bound",
    }

    FLOAT_DATATYPE = "float"
    FLOAT64_DATATYPE = "float64"
    INT_DATATYPE = "int"
    INT32_DATATYPE = "int32"
    STR_DATATYPE = "str"

    LAMBDA_EXPORT_OUTFILE = "/tmp/base_impedance_calculation.csv"
    S3_EXPORT_OUTFILE = "base_impedance_calculation/base_impedance_calculation.csv"

    BULKLOAD_JSON = {
            "source" : f"s3://{S3_UPLOAD_BUCKET}/base_impedance_calculation",
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
        "ENV": ENV,
        "QUERY_URL": QUERY_URL,
        "LOADER_URL": LOADER_URL,
        "DATASETID": DATASETID,
        "S3_UPLOAD_BUCKET": S3_UPLOAD_BUCKET,
        "ADMIN_LOG_BUCKET": ADMIN_LOG_BUCKET,
        "NUM_TRAVEL_TYPES": NUM_TRAVEL_TYPES,
        "FACTORS_FILE_KEY": FACTORS_FILE_KEY,
        "MUL": MUL,
        "ADD": ADD,
        "DATETIME_FORMAT_WITH_DASH": DATETIME_FORMAT_WITH_DASH,
        "SIDEWALK_LABEL": SIDEWALK_LABEL,
        "DEFECT_LABEL": DEFECT_LABEL,
        "RAMP_LABEL": RAMP_LABEL,
        "CURB_LABEL": CURB_LABEL,
        "CURB_CUT_LABEL": CURB_CUT_LABEL,
        "CROSSING_LABEL": CROSSING_LABEL,
        "BUS_STOP_LABEL": BUS_STOP_LABEL,
        "BASE_IMPEDANCE_LABEL": BASE_IMPEDANCE_LABEL,
        "EXPORT_IMPEDANCE_FIELDS": EXPORT_IMPEDANCE_FIELDS,
        "IMPEDANCE_CALC_DATATYPES": IMPEDANCE_CALC_DATATYPES,
        "SIDEWALK_LINK_ATTRS": SIDEWALK_LINK_ATTRS,
        "FACTORS_FIELDS": FACTORS_FIELDS,
        "FLOAT_DATATYPE": FLOAT_DATATYPE,
        "FLOAT64_DATATYPE": FLOAT64_DATATYPE,
        "INT_DATATYPE": INT_DATATYPE,
        "INT32_DATATYPE": INT32_DATATYPE,
        "STR_DATATYPE": STR_DATATYPE,
        "LAMBDA_EXPORT_OUTFILE": LAMBDA_EXPORT_OUTFILE,
        "S3_EXPORT_OUTFILE": S3_EXPORT_OUTFILE,
        "BULKLOAD_JSON": BULKLOAD_JSON,
        "SUCCESS_MESSAGE": SUCCESS_MESSAGE,
        "STATUS": STATUS,
    }

    return PARAMS