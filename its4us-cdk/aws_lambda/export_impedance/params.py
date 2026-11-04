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
    
        QUERY_URL = input_params["stageVariables"]["QUERY_URL"]
        ADMIN_LOG_BUCKET = input_params["stageVariables"]["ADMIN_LOG_BUCKET"]
        PUBLIC_EXPORT_BUCKET = input_params["stageVariables"]["PUBLIC_EXPORT_BUCKET"]

        query_params = input_params.get("body", {}) or {} # codepipeline places "id" in "body"

    else:

        JOB_ID = ""        # set CodePipeline job id and name to empty
        FUNCTION_NAME = ""
        PIPELINE_NAME = ""

        QUERY_URL = os.environ.get("QUERY_URL")
        ADMIN_LOG_BUCKET = os.environ.get("ADMIN_LOG_BUCKET")
        
        query_params = event.get("queryStringParameters", {}) or {}

    DATASETID = query_params.get("id", "")

    print("DATASETID OR SEARCH AREA:", DATASETID)

    # Export impedance to PUBLIC_EXPORT_BUCKET
    PUBLIC_EXPORT_BUCKET = os.environ.get("PUBLIC_EXPORT_BUCKET")

    # set impedance csv columns that will be exported
    cols = "None,Some,Device,WChairM,WChairE,MScooter,LowVision,Blind," \
            + "Some-LowVision,Device-LowVision,WChairM-LowVision,WChairE-LowVision,MScooter-LowVision," \
            + "Some-Blind,Device-Blind,WChairM-Blind,WChairE-Blind,MScooter-Blind"
    allowed_columns = set(cols.split(","))
    if "columns" in query_params:
        IMPEDANCE_COLUMNS = query_params["columns"].split(",")
        if not IMPEDANCE_COLUMNS or not set(IMPEDANCE_COLUMNS).issubset(allowed_columns):
            raise ValueError("Unsupported impedance column")
    else:
        IMPEDANCE_COLUMNS = cols.split(",")

    # set up the full headers of the output csv file
    IMPEDANCE_HEADERS = "Timestamp,Upstream Node,Downstream Node,Way Id,Link Length," + ",".join(IMPEDANCE_COLUMNS)
    IMPEDANCE_HEADERS += "\n"
    
    # function-specific constants
    STUDY_AREA = {
        "34.0N84.4W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.4, "max_lon": -84.3},
        "33.8N84.4W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.4, "max_lon": -84.3},
        "33.9N84.4W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.4, "max_lon": -84.3},
        "33.8N84.1W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.1, "max_lon": -84.0},
        "34.0N84.3W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.3, "max_lon": -84.2},
        "33.8N84.3W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.3, "max_lon": -84.2},
        "33.9N84.3W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.3, "max_lon": -84.2},
        "33.8N84.2W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.2, "max_lon": -84.1},
        "33.9N84.2W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.2, "max_lon": -84.1},
        "34.0N84.2W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.2, "max_lon": -84.1},
        "33.9N84.1W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.1, "max_lon": -84.0},
        "34.0N84.1W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.1, "max_lon": -84.0},
        "33.9N84.0W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.0, "max_lon": -83.9},
        "34.0N84.0W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.0, "max_lon": -83.9},
    }

    STUDY_AREA_BOUNDARY = {
        "min_lat": "min_lat",
        "max_lat": "max_lat",
        "min_lon": "min_lon",
        "max_lon": "max_lon",
    }

    OSM_NODE_LABEL = "OSM-NODE"
    IMPEDANCE = "IMPEDANCE"
    IMPEDANCE_LINK_ATTRS = {
        "__datasetid": "__datasetid",
        "Timestamp": "Timestamp",
        "stmAdaPathLinkID": "stmAdaPathLinkID",
        "stmAdaPathLinkLength": "stmAdaPathLinkLength",
    }
    EXPORT_IMPEDANCE_FIELDS = {
        "Timestamp": "Timestamp",
        "Upstream Node": "Upstream Node",
        "Downstream Node": "Downstream Node",
        "Way Id": "Way Id",
        "Link Length": "Link Length",
    }

    # define the output file name: one in Lambda and another in S3
    LAMBDA_OUTFILE = "/tmp/export_impedance_" + DATASETID + ".csv"
    S3_OUTFILE = "export_impedance_" + DATASETID + ".csv"

    SUCCESS_MESSAGE = f"SUCCESS: WHOLE PROCESS IS COMPLETED FOR THE REQUEST"

    STATUS = {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", 
                    "Access-Control-Allow-Headers": "Content-Type", 
                    "Access-Control-Allow-Origin":"*", 
                    "Access-Control-Allow-Methods": "OPTIONS,POST,DELETE"},
        "body": json.dumps("STATUS MESSAGE.")
    }

    CODEPIPELINE_STATUS = {
        "jobId": JOB_ID,
        "functionName": FUNCTION_NAME,
        "pipelineName": PIPELINE_NAME,
        "status": "STATUS",
        "message": "STATUS MESSAGE.",
    }

    PARAMS = {
        "QUERY_URL": QUERY_URL,
        "DATASETID": DATASETID,
        "ADMIN_LOG_BUCKET": ADMIN_LOG_BUCKET,
        "PUBLIC_EXPORT_BUCKET": PUBLIC_EXPORT_BUCKET,
        "IMPEDANCE_COLUMNS": IMPEDANCE_COLUMNS,
        "IMPEDANCE_HEADERS": IMPEDANCE_HEADERS,
        "STUDY_AREA": STUDY_AREA,
        "STUDY_AREA_BOUNDARY": STUDY_AREA_BOUNDARY,
        "OSM_NODE_LABEL": OSM_NODE_LABEL,
        "IMPEDANCE": IMPEDANCE,
        "IMPEDANCE_LINK_ATTRS": IMPEDANCE_LINK_ATTRS,
        "EXPORT_IMPEDANCE_FIELDS": EXPORT_IMPEDANCE_FIELDS,
        "LAMBDA_OUTFILE": LAMBDA_OUTFILE,
        "S3_OUTFILE":S3_OUTFILE,
        "SUCCESS_MESSAGE": SUCCESS_MESSAGE,
        "STATUS": STATUS,
        "CODEPIPELINE_STATUS": CODEPIPELINE_STATUS,
    }

    return PARAMS
