import json
import requests
import numpy
import pandas as pd
from neo4j import GraphDatabase, RoutingControl
import datetime
import boto3
from time import time

numTravelTypes = -18

def lambda_handler(event, context):
    print("Start:",time())
    id = event['queryStringParameters']['id']

    LOADER_URL = event['stageVariables']['LOADER_URL']
    QUERY_URL = event['stageVariables']['QUERY_URL']
    LOAD_BUCKET = event['stageVariables']['LOAD_BUCKET']
    BULKLOAD_SOURCE = "s3://"+LOAD_BUCKET
    PUBLIC_BUCKET = event['stageVariables']['PUBLIC_BUCKET']

    AUTH = ("username", "password") # not used
    env = event['requestContext']['stage']
    if 'testWaze' in event['queryStringParameters'] and event['queryStringParameters']['testWaze'] == 1:
        testWaze = True
    else:
        testWaze = False

    print("Reading csv:",time())

    factors = pd.read_csv('factors.csv', na_values='NA')
    travelTypes = factors.columns.tolist()[numTravelTypes:]
    ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("Querying database:",time())

    with GraphDatabase.driver(QUERY_URL, auth=AUTH, encrypted=True) as driver:
        print("Creating sidewalk query")
        query = "MATCH (na)-[s:`BASE-IMPEDANCE`]->(nb) WHERE s.`__datasetid` = '{}' RETURN ".format(id)
        query += ",".join(["s.`{0}` as `{0}`".format(travelType) for travelType in travelTypes])
        query += ",s.stmAdaPathLinkLength as stmAdaPathLinkLength,s.stmAdaPathLinkID as stmAdaPathLinkID,ID(na),ID(nb)"
        # print(query)
        # print("executing query")
        baseImpedance, _, _ = driver.execute_query(query)

        if not testWaze:
            print("Creating waze query")
            query = "MATCH (way:`OSM-WAY`)-[r:`WAZE-ALERT`]->(waze:`WAZE-ALERT`) "
            query += " RETURN way.id as stmAdaPathLinkID,r.__impedance_factor as factor,r.__impedance_effect_type as type"
            # print(query)
            # print("executing query")
            waze, _, _ = driver.execute_query(query)

        print("Finished query execution")

    if testWaze:
        print("Testing waze")
        waze_df = pd.read_csv('testWaze.csv', na_values='NA')
    else:
        waze_dict = [record.data() for record in waze]
        waze_df = pd.DataFrame(waze_dict)

    ##################################################################
    # Calculate impedance    

    print("Creating factor tables:",time())

    base_impedance_dict = [record.data() for record in baseImpedance]
    base_impedance_df = pd.DataFrame(base_impedance_dict)
    base_impedance_df['stmAdaPathLinkID'] = base_impedance_df['stmAdaPathLinkID'].astype('int').astype('str')
    base_impedance_df[travelTypes] = base_impedance_df[travelTypes].astype('float')

    print("Creating waze tables:",time())

    waze_df['stmAdaPathLinkID'] = waze_df['stmAdaPathLinkID'].astype('int').astype('str')
    waze_df['factor'] = waze_df['factor'].astype('float')

    if not waze_df.empty:
        waze_df[travelTypes] = pd.DataFrame([waze_df['factor'].values for travelType in travelTypes]).T
        mul_waze = waze_df[waze_df['type'] == 'MUL'].groupby('stmAdaPathLinkID', as_index=False).prod(numeric_only=True)
        add_waze = waze_df[waze_df['type'] == 'ADD'].groupby('stmAdaPathLinkID', as_index=False).sum(numeric_only=True)
    else:
        mul_waze = pd.DataFrame(columns=['stmAdaPathLinkID'])
        add_waze = pd.DataFrame(columns=['stmAdaPathLinkID'])

    # print("waze tables")
    # print(mul_waze)
    # print(add_waze)

    def apply_factors(row):    
        filtered_mul_waze = mul_waze[mul_waze['stmAdaPathLinkID']==row['stmAdaPathLinkID']].reset_index()
        if not filtered_mul_waze.empty:
            print(mul_waze[mul_waze['stmAdaPathLinkID']==row['stmAdaPathLinkID']].reset_index().iloc[0][travelTypes])
            print(row[travelTypes])
            row[travelTypes] = row[travelTypes] * mul_waze[mul_waze['stmAdaPathLinkID']==row['stmAdaPathLinkID']].reset_index().iloc[0][travelTypes]
        filtered_add_waze = add_waze[add_waze['stmAdaPathLinkID']==row['stmAdaPathLinkID']].reset_index()
        if not filtered_add_waze.empty:
            row[travelTypes] = row[travelTypes] + add_waze[add_waze['stmAdaPathLinkID']==row['stmAdaPathLinkID']].reset_index().iloc[0][travelTypes]

        return row

    print("Filtering sidewalks:",time())

    impedance = base_impedance_df.apply(apply_factors, axis='columns')
    
    ##################################################################
    # Format and upload
    
    print("Creating outputs:",time())
    impedance = impedance.rename(columns={
            'stmAdaPathLinkID':'stmAdaPathLinkID:String(single)',
            'stmAdaPathLinkLength':'stmAdaPathLinkLength:String(single)',
            'ID(na)':'~from',
            'ID(nb)':'~to'
        } | {travelType: travelType+":String(single)" for travelType in travelTypes})

    impedance = impedance.astype({'stmAdaPathLinkID:String(single)': 'int32'})
    impedance = impedance.astype({
        'stmAdaPathLinkID:String(single)': 'str',
        '~from': 'str',
        '~to': 'str'
        })

    impedance['~id']   = 'i' + impedance['stmAdaPathLinkID:String(single)'].astype('str') + '-' + impedance['~from'].astype('str') + '-' + impedance['~to'].astype('str')
    impedance['~label'] = 'IMPEDANCE'
    impedance['__datasetid:String(single)'] = id
    impedance['Timestamp:String(single)'] = ct
    impedance = impedance.drop(impedance[impedance['~id'] == 'i0-0-0'].index)
    impedance = impedance.drop(impedance[impedance['~from'].astype('str') == ''].index)
    impedance = impedance.drop(impedance[impedance['~to'].astype('str') == ''].index)
    impedance = impedance.drop(impedance[impedance['~from'].astype('str') == '0'].index)
    impedance = impedance.drop(impedance[impedance['~to'].astype('str') == '0'].index)

    impedance = impedance[['~id',
        '~from',
        '~to',
        '~label',
        '__datasetid:String(single)',
        'Timestamp:String(single)',
        'stmAdaPathLinkID:String(single)',
        'stmAdaPathLinkLength:String(single)']+[travelType+":String(single)" for travelType in travelTypes]]
    
    # # Add reverse direction
    # cols = list(impedance)
    # cols[2], cols[1] = cols[1], cols[2]
    # impedanceB_A = impedance.loc[:,cols]
    # cols[2], cols[1] = cols[1], cols[2]
    # impedanceB_A.columns = cols
    # impedanceB_A['~id'] = 'i' + impedance['stmAdaPathLinkID:String(single)'].astype('str') + '-' + impedanceB_A['~from'].astype('str') + '-' + impedanceB_A['~to'].astype('str')
    # impedance = pd.concat([impedance,impedanceB_A])

    impedance.to_csv("/tmp/impedance_calculation.csv",index=False,float_format='%.2f')
    s3 = boto3.client('s3')
    s3.upload_file("/tmp/impedance_calculation.csv", LOAD_BUCKET, "impedance_calculation/impedance_calculation.csv")

    bulkLoad_json = {
            "source" : BULKLOAD_SOURCE+"/impedance_calculation",
            "format" : "csv",
            "iamRoleArn" : "arn:aws:iam::760336115441:role/neptune-s3-read-access",
            "region" : "us-east-2",
            "failOnError" : "FALSE",
            "parallelism" : "MEDIUM",
            "updateSingleCardinalityProperties" : "TRUE",
            "queueRequest" : "TRUE",
            "dependencies" : []
        }

    bulkLoad_response = requests.post(LOADER_URL, json=bulkLoad_json)

    if env == "prod":
        impedance = impedance[[
            'Timestamp:String(single)',
            '~from',
            '~to',
            'stmAdaPathLinkID:String(single)',
            'stmAdaPathLinkLength:String(single)']+[travelType+":String(single)" for travelType in travelTypes]]
        impedance = impedance.rename(columns={
            'Timestamp:String(single)': 'Timestamp',
            '~from': 'Upstream Node',
            '~to': 'Downstream Node',
            'stmAdaPathLinkID:String(single)': 'Way Id',
            'stmAdaPathLinkLength:String(single)': 'Link Length',
            'None:String(single)': 'None',
            'Some:String(single)': 'Some',
            'Device:String(single)': 'Device',
            'WChairM:String(single)': 'WChairM',
            'WChairE:String(single)': 'WChairE',
            'MScooter:String(single)': 'MScooter',
            'LowVision:String(single)': 'LowVision',
            'Blind:String(single)': 'Blind',
            'Some-LowVision:String(single)': 'Some-LowVision',
            'Device-LowVision:String(single)': 'Device-LowVision',
            'WChairM-LowVision:String(single)': 'WChairM-LowVision',
            'WChairE-LowVision:String(single)': 'WChairE-LowVision',
            'MScooter-LowVision:String(single)': 'MScooter-LowVision',
            'Some-Blind:String(single)': 'Some-Blind',
            'Device-Blind:String(single)': 'Device-Blind',
            'WChairM-Blind:String(single)': 'WChairM-Blind',
            'WChairE-Blind:String(single)': 'WChairE-Blind',
            'MScooter-Blind:String(single)': 'MScooter-Blind'
        })
        
        impedance['Upstream Node'] = impedance['Upstream Node'].str[1:]
        impedance['Downstream Node'] = impedance['Downstream Node'].str[1:]
        
        impedance.to_csv("/tmp/impedance_export.csv",index=False,float_format='%.2f')
        
        s3.upload_file("/tmp/impedance_export.csv", PUBLIC_BUCKET, "impedance_export.csv")

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 
                    'Access-Control-Allow-Headers': 'Content-Type', 
                    'Access-Control-Allow-Origin':'*', 
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
        'body': json.dumps(bulkLoad_response.json())
    }

    