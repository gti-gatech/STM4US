import csv
import boto3
import traceback
import pandas as pd
from io import StringIO

def createBulkLoadCSV(PARAMS):

    s3 = boto3.client("s3")
    VDS_DATA_FIELDS = PARAMS["VDS_DATA_FIELDS"]

    try:
        print("Downloading from S3 and extracting...")
        object = s3.get_object(Bucket = PARAMS["NAVIGATOR_BUCKET"], 
                               Key = PARAMS["VDS_DATA_FILENAME"]) # e.g. its4us-navigator/vds_data_20260324T0815.csv

    except Exception as error:
        error_string = traceback.format_exc()
        print(f"TRACEBACK:\n{error_string}")

        return False
        
    data = object["Body"].read().decode()
    data = "\n".join(data.split("\r\n")[4:-2])
    
    with open(PARAMS["VDS_DEVICE_LOOKUP_FILE"]) as f:
        next(f)  # Skip the header
        reader = csv.reader(f, skipinitialspace=True)
        links = dict(reader)

    df = pd.read_csv(StringIO(data))

    df = df[df["device_id"].astype(str).isin(links.keys())]
    
    df.insert(loc = 0 , column = VDS_DATA_FIELDS["~label"], value = PARAMS["VDS"])
    df.insert(loc = 0, column = VDS_DATA_FIELDS["~id"], 
              value = df[VDS_DATA_FIELDS["device_id"]].astype(str)
              + "-" + df[VDS_DATA_FIELDS["detector_id"]].astype(str) 
              + "-" + df[VDS_DATA_FIELDS["timestamp"]].astype(str).replace(" ", "-"))
    
    df = df.rename(columns = PARAMS["COLUMNS"])

    df = df.astype({PARAMS["COLUMNS"]["volume"]: PARAMS["INT64_DATATYPE"], 
                    PARAMS["COLUMNS"]["occupancy"]: PARAMS["INT64_DATATYPE"]})
    
    osmLinks = df[PARAMS["COLUMNS"]["device_id"]].astype(str).map(links).fillna("").astype(str)
    
    rel_df = df[[VDS_DATA_FIELDS["~id"]]].copy()
    rel_df.rename(columns = {VDS_DATA_FIELDS["~id"]: VDS_DATA_FIELDS["~to"]}, inplace = True)
    rel_df.insert(loc = 0, column = VDS_DATA_FIELDS["~from"], value = "w" + osmLinks)
    rel_df.insert(loc = 0, column = VDS_DATA_FIELDS["~id"], 
                  value = PARAMS["LINK"] + "-" + df[VDS_DATA_FIELDS["~id"]])

    # save the processed VDS data to S3 bucket
    LOCAL_NODE_PATH = "/tmp/" + PARAMS["VDS_NODE_FILENAME"]
    LOCAL_LINK_PATH = "/tmp/" + PARAMS["VDS_LINK_FILENAME"]
    df.to_csv(LOCAL_NODE_PATH, header = True, index = False, mode = "a")
    rel_df.to_csv(LOCAL_LINK_PATH, header = True, index = False, mode = "a")

    S3_NODE_PATH = PARAMS["S3_UPLOAD_FOLDER"] + "/" + PARAMS["VDS_NODE_FILENAME"]
    S3_LINK_PATH = PARAMS["S3_UPLOAD_FOLDER"] + "/" + PARAMS["VDS_LINK_FILENAME"]
    s3.upload_file(LOCAL_NODE_PATH, PARAMS["S3_UPLOAD_BUCKET"], S3_NODE_PATH)
    s3.upload_file(LOCAL_LINK_PATH, PARAMS["S3_UPLOAD_BUCKET"], S3_LINK_PATH)

    message = "DONE: The processed VDS data are uploaded to " + PARAMS["S3_UPLOAD_BUCKET"] \
        + " in the files: " + S3_NODE_PATH + " and " + S3_LINK_PATH
    print(message)

    return True