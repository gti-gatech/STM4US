import requests
import boto3
import pandas as pd

def generate_impedance_output(PARAMS, travelTypes, currentTime, impedance):

    EXPORT_IMPEDANCE_FIELDS = PARAMS["EXPORT_IMPEDANCE_FIELDS"]
    IMPEDANCE_CALC_DATATYPES = PARAMS["IMPEDANCE_CALC_DATATYPES"]


    impedance = impedance.rename(columns = IMPEDANCE_CALC_DATATYPES["COLUMNS"] 
                                 | {travelType: travelType+":String(single)" 
                                    for travelType in travelTypes})

    impedance = impedance.astype({IMPEDANCE_CALC_DATATYPES["COLUMNS"]["stmAdaPathLinkID"]: 
                                  PARAMS["INT32_DATATYPE"]})

    impedance[EXPORT_IMPEDANCE_FIELDS["~id"]] = "bi" \
        + impedance[IMPEDANCE_CALC_DATATYPES["COLUMNS"]["stmAdaPathLinkID"]]. \
            astype(PARAMS["STR_DATATYPE"]) + "-" + impedance[EXPORT_IMPEDANCE_FIELDS["~from"]]. \
                astype(PARAMS["STR_DATATYPE"]) + "-" + impedance[EXPORT_IMPEDANCE_FIELDS["~to"]]. \
                    astype(PARAMS["STR_DATATYPE"])
    impedance[EXPORT_IMPEDANCE_FIELDS["~label"]] = PARAMS["BASE_IMPEDANCE_LABEL"]
    impedance[IMPEDANCE_CALC_DATATYPES["DATASETID"]] = PARAMS["DATASETID"]
    impedance[IMPEDANCE_CALC_DATATYPES["TIMESTAMP"]] = currentTime
    impedance = impedance.drop(impedance[impedance[EXPORT_IMPEDANCE_FIELDS["~id"]] == "bi0-0-0"].index)
    impedance = impedance.drop(impedance[impedance[EXPORT_IMPEDANCE_FIELDS["~from"]]. \
                                         astype(PARAMS["STR_DATATYPE"]) == ""].index)
    impedance = impedance.drop(impedance[impedance[EXPORT_IMPEDANCE_FIELDS["~to"]]. \
                                         astype(PARAMS["STR_DATATYPE"]) == ""].index)
    impedance = impedance.drop(impedance[impedance[EXPORT_IMPEDANCE_FIELDS["~from"]]. \
                                         astype(PARAMS["STR_DATATYPE"]) == "0"].index)
    impedance = impedance.drop(impedance[impedance[EXPORT_IMPEDANCE_FIELDS["~to"]]. \
                                         astype(PARAMS["STR_DATATYPE"]) == "0"].index)

    impedance = impedance[[
        EXPORT_IMPEDANCE_FIELDS["~id"],
        EXPORT_IMPEDANCE_FIELDS["~from"],
        EXPORT_IMPEDANCE_FIELDS["~to"],
        EXPORT_IMPEDANCE_FIELDS["~label"],
        IMPEDANCE_CALC_DATATYPES["DATASETID"],
        IMPEDANCE_CALC_DATATYPES["TIMESTAMP"],
        IMPEDANCE_CALC_DATATYPES["COLUMNS"]["stmAdaPathLinkID"],
        IMPEDANCE_CALC_DATATYPES["COLUMNS"]["stmAdaPathLinkLength"]]
        + [travelType+":String(single)" for travelType in travelTypes]]
    
    # add reverse direction
    cols = list(impedance)
    cols[2], cols[1] = cols[1], cols[2]
    impedanceB_A = impedance.loc[:,cols]
    cols[2], cols[1] = cols[1], cols[2]
    impedanceB_A.columns = cols
    impedanceB_A[EXPORT_IMPEDANCE_FIELDS["~id"]] = "bi" + impedance[IMPEDANCE_CALC_DATATYPES["COLUMNS"]["stmAdaPathLinkID"]]. \
        astype(PARAMS["STR_DATATYPE"]) + "-" + impedanceB_A[EXPORT_IMPEDANCE_FIELDS["~from"]]. \
            astype(PARAMS["STR_DATATYPE"]) + "-" + impedanceB_A[EXPORT_IMPEDANCE_FIELDS["~to"]]. \
                astype(PARAMS["STR_DATATYPE"])
    
    impedance = pd.concat([impedance,impedanceB_A])

    # upload the impedance calculate output file to s3
    impedance.to_csv(PARAMS["LAMBDA_EXPORT_OUTFILE"], index = False)
    s3 = boto3.client("s3")
    s3.upload_file(PARAMS["LAMBDA_EXPORT_OUTFILE"], PARAMS["S3_UPLOAD_BUCKET"], PARAMS["S3_EXPORT_OUTFILE"])

    # POST the impedance calculate output to database
    bulkLoad_response = requests.post(PARAMS["LOADER_URL"], json = PARAMS["BULKLOAD_JSON"])

    return bulkLoad_response