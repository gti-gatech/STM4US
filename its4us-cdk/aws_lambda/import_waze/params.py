import json
from datetime import datetime
import os

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
    
        METHOD = input_params["httpMethod"]
        ADMIN_LOG_BUCKET = input_params["stageVariables"]["ADMIN_LOG_BUCKET"]
        WAZE_BUCKET = input_params["stageVariables"]["UPLOAD_BUCKET"]
        WAZE_FOLDER = input_params["stageVariables"]["WAZE_FOLDER"]
        QUERY_URL = input_params["stageVariables"]["QUERY_URL"]

        if METHOD == "POST":
            DATASETID = str(input_params["body"]["id"])
        else:
            DATASETID = "" # empty for DELETE
            
    else:

        JOB_ID = ""        # set CodePipeline job id and name to empty
        FUNCTION_NAME = ""
        PIPELINE_NAME = ""
    
        METHOD = event.get("httpMethod")
        
        if METHOD == "POST":
            # extract data_set_id input parameter
            body = event.get("body", {})
            if isinstance(body, str):
                event_body = json.loads(body)
                DATASETID = str(event_body.get("id", ""))
            else:
                # a dict data type here
                DATASETID = str(body.get("id", ""))
        else:
            DATASETID = "" # empty for DELETE

        ADMIN_LOG_BUCKET = os.environ.get("ADMIN_LOG_BUCKET")

        # upload Waze data to UPLOAD_BUCKET/WAZE_FOLDER
        WAZE_BUCKET = os.environ.get("UPLOAD_BUCKET")
        WAZE_FOLDER = os.environ.get("WAZE_FOLDER")
        
        QUERY_URL = os.environ.get("QUERY_URL")

    print("METHOD USED:", METHOD)
    print("DATASETID (EMPTY IF METHOD IS DELETE):", DATASETID)

    # Feed URLs are deployment configuration, supplied by CDK as JSON.
    WAZE_URLS = json.loads(os.environ["WAZE_URLS"])
    WAZE_DATA_FIELDS = {
        "alerts": "alerts",
        "startTimeMillis": "startTimeMillis",
        "endTimeMillis": "endTimeMillis",
        "startTime": "startTime",
        "endTime": "endTime",
    }

    WAZE_ALERT_FIELDS = {
        "location": "location",
        "x": "x",
        "y": "y",
        "uuid": "uuid",
        "type": "type",
        "subtype": "subtype",
        "roadType": "roadType",
        "endTime": "endTime",
        "endTimeMillis": "endTimeMillis",
    }

    WAZE_NODE_ATTRS = {
        "location-x": "location-x",
        "location-y": "location-y",
        "__datasetid": "__datasetid",
    }

    WAZE_LINK_ATTRS = {
        "__datasetid": "__datasetid",
        "__impedance_factor": "__impedance_factor",
        "__impedance_effect_type": "__impedance_effect_type",
    }
    
    NO_SUBTYPE = "NO_SUBTYPE"
    EFFECT_TYPE = "EFFECT_TYPE"
    HAZARD_WEATHER = "HAZARD_WEATHER"
    JAM = "JAM"
    WAZE_NODE_LABEL = "WAZE-ALERT"
    OSM_NODE_LABEL = "OSM-NODE"
    OSM_WAY_LABEL = "OSM-WAY"
    SIDEWALK_LABEL = "SIDEWALK"
    CROSSWALK_LABEL = "CROSSWALK"

    WAZE_HOLDTIME = 900000 # holding waze data for 15 minutes or 900000 ms for DELETE request
    WEATHER_DISTANCE = 1000.0 # distance (ft) boundary for sidewalk/crosswalk node attachments on weather hazard
    INTERSECTION_BOX = 300.0 # distance (ft) boundary for creating an intersection
    CROSSWALK_BOX_OUTSIDE = 80.0 # distance (ft) boundary for crosswalk node attachments on outside of intersection box
    CROSSWALK_BOX_OUTSIDE_SIDEWALK = 50.0 # distance (ft) boundary for sidealk node attachments on outside of intersection box
    CROSSWALK_BOX_INSIDE = 20.0 # distance (ft) boundary for crosswalk node attachments on inside of intersection box
    METER_TO_FEET = 3.28084 # 1 meter is 3.28084 feet

    GEOSPACE = "WGS84"

    # sidewalk and crosswalk queries
    sidewalk_query1 = "MATCH (node1:`OSM-NODE`)-[:FIRST]-(sidewalk:`OSM-WAY` {footway: 'sidewalk', __datasetid: $dataset_id})-"
    sidewalk_query2 = "[:LAST]-(node2:`OSM-NODE`) RETURN sidewalk, node1, node2"
    SIDEWALK_QUERY = sidewalk_query1 + sidewalk_query2

    crosswalk_query1 = "MATCH (node1:`OSM-NODE`)-[:FIRST]-(crosswalk:`OSM-WAY` {footway: 'crossing', __datasetid: $dataset_id})-"
    crosswalk_query2 = "[:LAST]-(node2:`OSM-NODE`) RETURN crosswalk, node1, node2"
    CROSSWALK_QUERY = crosswalk_query1 + crosswalk_query2

    # find current datetime to be used as part of the filename that will save the waze data
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    datetime_str = now.strftime("%Y-%m-%d-%H%M%S")

    # create the filename to save the waze data
    WAZE_FILEPATH = WAZE_FOLDER + "/" + date_str + "/" + DATASETID + "/" + DATASETID + "-" + datetime_str + ".json"

    WAZE_MESSAGE = f"RETRIEVED: Waze alerts for study area {DATASETID}"
    S3_UPLOAD_MESSAGE = f"Uploading Waze alerts data to S3 bucket: {WAZE_BUCKET} " \
        + f"in the file: {WAZE_FILEPATH}"
    S3_FINISH_MESSAGE = f"Done uploading Waze alerts data to S3 bucket: {WAZE_BUCKET} " \
        + f"in the file: {WAZE_FILEPATH}"

    WARNING_MESSAGE = "WARNING: NO WAZE ALERTS DATA FOUND FROM " \
        + "THE WAZE ENDPOINT FOR STUDY AREA: " + DATASETID

    SUCCESS_MESSAGE = f"SUCCESS: WHOLE PROCESS IS COMPLETED FOR THE {METHOD} REQUEST" 
    
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
        "METHOD": METHOD,
        "DATASETID": DATASETID,
        "ADMIN_LOG_BUCKET": ADMIN_LOG_BUCKET,
        "WAZE_BUCKET": WAZE_BUCKET,
        "QUERY_URL": QUERY_URL,
        "WAZE_URLS": WAZE_URLS,
        "WAZE_DATA_FIELDS": WAZE_DATA_FIELDS,
        "WAZE_ALERT_FIELDS": WAZE_ALERT_FIELDS,
        "WAZE_NODE_ATTRS": WAZE_NODE_ATTRS,
        "WAZE_LINK_ATTRS": WAZE_LINK_ATTRS,
        "WAZE_NODE_LABEL": WAZE_NODE_LABEL,
        "NO_SUBTYPE": NO_SUBTYPE,
        "EFFECT_TYPE": EFFECT_TYPE,
        "HAZARD_WEATHER": HAZARD_WEATHER,
        "JAM": JAM,
        "OSM_NODE_LABEL": OSM_NODE_LABEL,
        "OSM_WAY_LABEL": OSM_WAY_LABEL,
        "SIDEWALK_LABEL": SIDEWALK_LABEL,
        "CROSSWALK_LABEL": CROSSWALK_LABEL,
        "WAZE_HOLDTIME": WAZE_HOLDTIME,
        "WEATHER_DISTANCE": WEATHER_DISTANCE,
        "INTERSECTION_BOX": INTERSECTION_BOX,
        "CROSSWALK_BOX_OUTSIDE": CROSSWALK_BOX_OUTSIDE,
        "CROSSWALK_BOX_OUTSIDE_SIDEWALK": CROSSWALK_BOX_OUTSIDE_SIDEWALK,
        "CROSSWALK_BOX_INSIDE": CROSSWALK_BOX_INSIDE,
        "METER_TO_FEET": METER_TO_FEET,
        "GEOSPACE": GEOSPACE,
        "SIDEWALK_QUERY": SIDEWALK_QUERY,
        "CROSSWALK_QUERY": CROSSWALK_QUERY,
        "DATASET_QUERY_PARAMS": {"dataset_id": DATASETID},
        "WAZE_FILEPATH": WAZE_FILEPATH,
        "WAZE_MESSAGE": WAZE_MESSAGE,
        "S3_UPLOAD_MESSAGE": S3_UPLOAD_MESSAGE,
        "S3_FINISH_MESSAGE": S3_FINISH_MESSAGE,
        "WARNING_MESSAGE": WARNING_MESSAGE,
        "SUCCESS_MESSAGE": SUCCESS_MESSAGE,
        "STATUS": STATUS,
        "CODEPIPELINE_STATUS": CODEPIPELINE_STATUS,
    }

    return PARAMS
