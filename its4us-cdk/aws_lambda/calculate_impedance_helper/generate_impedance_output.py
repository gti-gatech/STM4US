import boto3
import requests
from time import time


def upload_to_s3(lambdaOutfile, s3Bucket, s3Outfile, impedance, s3):

    # upload file to s3
    impedance.to_csv(lambdaOutfile, index = False, float_format = "%.2f")
    s3.upload_file(lambdaOutfile, s3Bucket, s3Outfile)

    return

def generate_impedance_output(PARAMS, travelTypes, currentTime, impedance):

    print("Creating outputs:", time())

    IMPEDANCE_CALC_DATATYPES = PARAMS["IMPEDANCE_CALC_DATATYPES"]
    EXPORT_IMPEDANCE_FIELDS = PARAMS["EXPORT_IMPEDANCE_FIELDS"]

    print("Creating outputs:", time())
    impedance = impedance.rename(columns = IMPEDANCE_CALC_DATATYPES["COLUMNS"] 
                                 | {travelType: travelType+":String(single)" for travelType in travelTypes})

    impedance = impedance.astype({IMPEDANCE_CALC_DATATYPES["COLUMNS"]["stmAdaPathLinkID"]: 'int32'})
    impedance = impedance.astype({
        IMPEDANCE_CALC_DATATYPES["COLUMNS"]["stmAdaPathLinkID"]: "str",
        EXPORT_IMPEDANCE_FIELDS["~from"]: 'str',
        EXPORT_IMPEDANCE_FIELDS["~to"]: 'str'
        })

    impedance[EXPORT_IMPEDANCE_FIELDS["~id"]] \
        = "i" + impedance[IMPEDANCE_CALC_DATATYPES["COLUMNS"]["stmAdaPathLinkID"]].astype("str") + "-" \
        + impedance[EXPORT_IMPEDANCE_FIELDS["~from"]].astype("str") + "-" + \
            impedance[EXPORT_IMPEDANCE_FIELDS["~to"]].astype('str')
    
    impedance[EXPORT_IMPEDANCE_FIELDS["~label"]] = PARAMS["IMPEDANCE_LABEL"]
    impedance[IMPEDANCE_CALC_DATATYPES["DATASETID"]] = PARAMS["DATASETID"]
    impedance[IMPEDANCE_CALC_DATATYPES["TIMESTAMP"]] = currentTime
    impedance = impedance.drop(impedance[impedance[EXPORT_IMPEDANCE_FIELDS["~id"]] == "i0-0-0"].index)
    impedance = impedance.drop(impedance[impedance[EXPORT_IMPEDANCE_FIELDS["~from"]].astype("str") == ""].index)
    impedance = impedance.drop(impedance[impedance[EXPORT_IMPEDANCE_FIELDS["~to"]].astype("str") == ""].index)
    impedance = impedance.drop(impedance[impedance[EXPORT_IMPEDANCE_FIELDS["~from"]].astype("str") == "0"].index)
    impedance = impedance.drop(impedance[impedance[EXPORT_IMPEDANCE_FIELDS["~to"]].astype("str") == "0"].index)

    impedance = impedance[[EXPORT_IMPEDANCE_FIELDS["~id"],
        EXPORT_IMPEDANCE_FIELDS["~from"],
        EXPORT_IMPEDANCE_FIELDS["~to"],
        EXPORT_IMPEDANCE_FIELDS["~label"],
        IMPEDANCE_CALC_DATATYPES["DATASETID"],
        IMPEDANCE_CALC_DATATYPES["TIMESTAMP"],
        IMPEDANCE_CALC_DATATYPES["COLUMNS"]["stmAdaPathLinkID"],
        IMPEDANCE_CALC_DATATYPES["COLUMNS"]["stmAdaPathLinkLength"]]
        + [travelType+":String(single)" for travelType in travelTypes]]
    
    # # Add reverse direction
    # cols = list(impedance)
    # cols[2], cols[1] = cols[1], cols[2]
    # impedanceB_A = impedance.loc[:,cols]
    # cols[2], cols[1] = cols[1], cols[2]
    # impedanceB_A.columns = cols
    # impedanceB_A['~id'] = 'i' + impedance['stmAdaPathLinkID:String(single)'].astype('str') + '-' + impedanceB_A['~from'].astype('str') + '-' + impedanceB_A['~to'].astype('str')
    # impedance = pd.concat([impedance,impedanceB_A])

    s3 = boto3.client("s3")

    # upload impedance calculation result to S3 (no public access) internally
    upload_to_s3(PARAMS["LAMBDA_CALC_OUTFILE"], PARAMS["UPLOAD_BUCKET"], 
                 PARAMS["S3_CALC_OUTFILE"], impedance, s3)

    # POST impedance calculation to Neptune database
    bulkLoad_response = requests.post(PARAMS["LOADER_URL"], json = PARAMS["BULKLOAD_JSON"])

    impedance = impedance[[
        IMPEDANCE_CALC_DATATYPES["TIMESTAMP"],
        EXPORT_IMPEDANCE_FIELDS["~from"],
        EXPORT_IMPEDANCE_FIELDS["~to"],
        IMPEDANCE_CALC_DATATYPES["COLUMNS"]["stmAdaPathLinkID"],
        IMPEDANCE_CALC_DATATYPES["COLUMNS"]["stmAdaPathLinkLength"]]
        + [travelType+":String(single)" for travelType in travelTypes]]
    
    impedance = impedance.rename(columns = PARAMS["IMPEDANCE_EXPORT_DATATYPES"])
    
    impedance[EXPORT_IMPEDANCE_FIELDS["Upstream Node"]] \
        = impedance[EXPORT_IMPEDANCE_FIELDS["Upstream Node"]].str[1:]
    impedance[EXPORT_IMPEDANCE_FIELDS["Downstream Node"]] \
        = impedance[EXPORT_IMPEDANCE_FIELDS["Downstream Node"]].str[1:]

    # upload impedance calculation result to S3 for review externally
    upload_to_s3(PARAMS["LAMBDA_EXPORT_OUTFILE"], PARAMS["PUBLIC_EXPORT_BUCKET"], 
                 PARAMS["S3_EXPORT_OUTFILE"], impedance, s3)

    message = "SUCCESS: Impedance calculation results are uploaded to S3: " + PARAMS["PUBLIC_EXPORT_BUCKET"] \
        + " in the file: " + "impedance_export.csv"
    print(message)

    return bulkLoad_response