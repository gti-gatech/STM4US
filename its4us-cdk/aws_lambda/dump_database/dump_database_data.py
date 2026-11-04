import boto3
import pandas as pd


def dump_data(PARAMS, sidewalks, defects, ramps, curbs, 
              curbCuts, crossings, busStops, baseImpedance):

    # upload queried data to S3 bucket: PUBLIC_EXPORT_BUCKET
    sidewalk_dict = [record.data() for record in sidewalks]
    sidewalk_df = pd.DataFrame(sidewalk_dict)
    sidewalk_df.to_csv(PARAMS["LAMBDA_SIDEWALKS_OUTFILE"], index=False)
    s3 = boto3.client('s3')
    s3.upload_file(PARAMS["LAMBDA_SIDEWALKS_OUTFILE"], PARAMS["PUBLIC_EXPORT_BUCKET"], 
                   PARAMS["S3_SIDEWALKS_OUTFILE"])

    for data, name in [(defects,"defects"), (ramps,"ramps"), (curbs,"curbs"), (curbCuts,"curbCuts"), 
                       (crossings,"crossings"), (busStops,"busStops")]:
        data_dict = [record.data()['n'] for record in data]
        data_df = pd.DataFrame(data_dict)
        data_df.to_csv(f'/tmp/{name}.csv',index=False)
        s3 = boto3.client('s3')
        s3.upload_file(f'/tmp/{name}.csv', PARAMS["PUBLIC_EXPORT_BUCKET"], f'dump/{name}.csv')

    # upload queried base impedance data to S3 bucket: PUBLIC_EXPORT_BUCKET
    baseImpedance_dict = [record.data() for record in baseImpedance]
    baseImpedance_df = pd.DataFrame(baseImpedance_dict)
    baseImpedance_df.to_csv(PARAMS["LAMBDA_IMPEDANCE_OUTFILE"], index=False)
    s3 = boto3.client('s3')
    s3.upload_file(PARAMS["LAMBDA_IMPEDANCE_OUTFILE"], PARAMS["PUBLIC_EXPORT_BUCKET"], 
                   PARAMS["S3_IMPEDANCE_OUTFILE"])

    return