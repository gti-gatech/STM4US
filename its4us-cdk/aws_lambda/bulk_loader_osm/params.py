import os

def find_params(event):

    PARAMS = dict()

    ENV = os.environ.get("STAGE")
    METHOD = event.get("httpMethod")
    QUERY_URL = os.environ.get("QUERY_URL")
    LOADER_URL = os.environ.get("LOADER_URL")
    S3_UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET")
    
    if METHOD == "PUT":
        DATASETID = ""
    else:
        query_params = event.get("queryStringParameters", {}) or {}
        DATASETID = query_params.get("id", "")

    print("ENV:", ENV)
    print("METHOD USED:", METHOD)
    print("DATASETID:", DATASETID)

    S3_UPLOAD_FOLDER = "osm-updated"
    OSM_DATAFILE = "osm-updated.xml"
    LAMBDA_OUTFILE = "/tmp/data.osm"

    NODE_OUTFILE = "node.csv"
    WAY_OUTFILE = "way.csv"
    RELATION_OUTFILE = "relation.csv"
    WAYLINK_OUTFILE = "wayLink.csv"
    RELATIONLINK_OUTFILE = "relationLink.csv"

    LAMBDA_NODE_OUTFILE = "/tmp/" + NODE_OUTFILE
    LAMBDA_WAY_OUTFILE = "/tmp/" + WAY_OUTFILE
    LAMBDA_RELATION_OUTFILE = "/tmp/" + RELATION_OUTFILE
    LAMBDA_WAYLINK_OUTFILE = "/tmp/" + WAYLINK_OUTFILE
    LAMBDA_RELATIONLINK_OUTFILE = "/tmp/" + RELATIONLINK_OUTFILE

    S3_NODE_OUTFILE = S3_UPLOAD_FOLDER + "/" + NODE_OUTFILE
    S3_WAY_OUTFILE = S3_UPLOAD_FOLDER + "/" + WAY_OUTFILE
    S3_RELATION_OUTFILE = S3_UPLOAD_FOLDER + "/" + RELATION_OUTFILE
    S3_WAYLINK_OUTFILE = S3_UPLOAD_FOLDER + "/" + WAYLINK_OUTFILE
    S3_RELATIONLINK_OUTFILE = S3_UPLOAD_FOLDER + "/" + RELATIONLINK_OUTFILE

    source = "s3://" + S3_UPLOAD_BUCKET + "/" + S3_UPLOAD_FOLDER
    BULKLOAD_JSON = {
        "source" : source,
        "format" : "csv",
        "iamRoleArn" : os.environ.get("NEPTUNE_S3_ROLE_ARN"),
        "region" : os.environ.get("AWS_REGION_NAME"),
        "failOnError" : "FALSE",
        "parallelism" : "MEDIUM",
        "updateSingleCardinalityProperties" : "TRUE",
        "queueRequest" : "TRUE",
        "dependencies" : []
    }

    MAIN_ELEMENTS = ["node", "way", "relation"]

    OSM_FIELDS = {
        "osm": "osm",
        "node": "node",
        "nodes": "nodes",
        "way": "way",
        "relation": "relation",
        "nd": "nd",
        "member": "member",
        "members": "members",
        "tag": "tag",
        "tags": "tags",
        "type": "type",
        "id": "id",
        "ref": "ref",
        "role": "role",
        "lat": "lat",
        "lon": "lon",
        "k": "k",
        "v": "v",
        "__datasetid": "__datasetid",
    }

    OSM_NODE_LABEL = "OSM-NODE"
    OSM_WAY_LABEL = "OSM-WAY"
    OSM_RELATION_LABEL = "OSM-RELATION"

    PARAMS = {
        "ENV": ENV,
        "METHOD": METHOD,
        "QUERY_URL": QUERY_URL,
        "LOADER_URL": LOADER_URL,
        "S3_UPLOAD_BUCKET": S3_UPLOAD_BUCKET,
        "DATASETID": DATASETID,
        "S3_UPLOAD_FOLDER": S3_UPLOAD_FOLDER,
        "LAMBDA_OUTFILE": LAMBDA_OUTFILE,
        "OSM_DATAFILE": OSM_DATAFILE,
        "NODE_OUTFILE": NODE_OUTFILE,
        "WAY_OUTFILE": WAY_OUTFILE,
        "RELATION_OUTFILE": RELATION_OUTFILE,
        "WAYLINK_OUTFILE": WAYLINK_OUTFILE,
        "RELATIONLINK_OUTFILE": RELATIONLINK_OUTFILE,
        "LAMBDA_NODE_OUTFILE": LAMBDA_NODE_OUTFILE,
        "LAMBDA_WAY_OUTFILE": LAMBDA_WAY_OUTFILE,
        "LAMBDA_RELATION_OUTFILE": LAMBDA_RELATION_OUTFILE,
        "LAMBDA_WAYLINK_OUTFILE": LAMBDA_WAYLINK_OUTFILE,
        "LAMBDA_RELATIONLINK_OUTFILE": LAMBDA_RELATIONLINK_OUTFILE,
        "S3_NODE_OUTFILE": S3_NODE_OUTFILE,
        "S3_WAY_OUTFILE": S3_WAY_OUTFILE,
        "S3_RELATION_OUTFILE": S3_RELATION_OUTFILE,
        "S3_WAYLINK_OUTFILE": S3_WAYLINK_OUTFILE,
        "S3_RELATIONLINK_OUTFILE": S3_RELATIONLINK_OUTFILE,
        "BULKLOAD_JSON": BULKLOAD_JSON,
        "MAIN_ELEMENTS": MAIN_ELEMENTS,
        "OSM_FIELDS": OSM_FIELDS,
        "OSM_NODE_LABEL": OSM_NODE_LABEL,
        "OSM_WAY_LABEL": OSM_WAY_LABEL,
        "OSM_RELATION_LABEL": OSM_RELATION_LABEL,

    }

    return PARAMS