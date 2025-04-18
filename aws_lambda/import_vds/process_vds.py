import json
import zlib
import boto3
import io
from io import StringIO
import pandas as pd
import csv

def createBulkLoadCSV(filename, bucket_name):
    s3 = boto3.client('s3')

    s3_Bucket_Name = bucket_name
    s3_File_Name = filename

    print("Downloading from S3 and extracting...") #Trying to save memory here
    object = s3.get_object(Bucket=s3_Bucket_Name, Key=s3_File_Name)
    data = object['Body'].read().decode()
    data = '\n'.join(data.split("\r\n")[4:-2])
    
    with open('VDS_Devices_OSM_Links.csv') as f:
        next(f)  # Skip the header
        reader = csv.reader(f, skipinitialspace=True)
        links = dict(reader)

    df = pd.read_csv(StringIO(data))

    df = df[df['device_id'].astype(str).isin(links.keys())]
    # df = df.loc[(df['latitude'] > 33.75) & (df['latitude'] < 34.15) & (df['longitude'] > -84.35) & (df['longitude'] < 83.85)]
    
    df.insert(loc=0, column='~label', value="VDS")
    df.insert(loc=0, column='~id', value=df['device_id'].astype(str)+ '-' + df['detector_id'].astype(str) + '-' + df['timestamp'].astype(str).replace(' ','-'))
    df = df.rename(columns={
        'device_id': 'device_id:String(single)',
        'detector_id': 'detector_id:String(single)',
        'timestamp': 'timestamp:String(single)',
        'status': 'status:String(single)',
        'volume': 'volume:Int(single)',
        'speed': 'speed:Float(single)',
        'occupancy': 'occupancy:Int(single)',
        'confidence': 'confidence:String(single)',
        'external_id': 'external_id:String(single)',
        'latitude': 'latitude:Float(single)',
        'longitude': 'longitude:Float(single)' 
    })
    df = df.astype({'volume:Int(single)':'Int64','occupancy:Int(single)':'Int64'})
    
    osmLinks = df['device_id:String(single)'].astype(str).map(links).fillna('').astype(str)
    
    rel_df = df[['~id']].copy()
    rel_df.rename(columns={'~id':'~to'},inplace=True)
    rel_df.insert(loc=0, column='~from', value='w'+osmLinks)
    rel_df.insert(loc=0, column='~id', value='link-'+df['~id'])

    df.to_csv("/tmp/vds_nodes.csv",header=True,index=False,mode='a')
    rel_df.to_csv("/tmp/vds_links.csv",header=True,index=False, mode='a')
    
    s3.upload_file("/tmp/vds_nodes.csv", bucket_name, "bulk_loader/vds_nodes.csv")
    s3.upload_file("/tmp/vds_links.csv", bucket_name, "bulk_loader/vds_links.csv")