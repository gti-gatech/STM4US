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
    # print("Start:",time())
    id = event['queryStringParameters']['id']
    env = event['requestContext']['stage']

    LOADER_URL = event['stageVariables']['LOADER_URL']
    QUERY_URL = event['stageVariables']['QUERY_URL']
    LOAD_BUCKET = event['stageVariables']['LOAD_BUCKET']

    AUTH = ("username", "password") # not used

    # print("Querying database:",time())

    # print("Starting driver")
    with GraphDatabase.driver(QUERY_URL, auth=AUTH, encrypted=True) as driver:
        # print("Creating sidewalk query")
        query = "MATCH (na)-[s:`GT/CE-SIDEWALK`]->(nb) WHERE s.`__datasetid` = '{}' RETURN ".format(id)
        query += "s.stmAdaPathLinkID as stmAdaPathLinkID, s.stmAdaPathLinkLength as stmAdaPathLinkLength,ID(na),ID(nb)"
        # print(query)
        # print("executing query")
        sidewalks, _, _ = driver.execute_query(query)

        # print("Creating defects query")
        query = "MATCH (s)-[l:`GT/CE-SIDEWALK-DEFECT`]->(n) RETURN s,n"
        # print(query)
        # print("executing query")
        defects, _, _ = driver.execute_query(query)

        # print("Creating ramps query")
        query = "MATCH (s)-[l:`GT/CE-SIDEWALK-RAMP`]->(n) RETURN s,n"
        # print(query)
        # print("executing query")
        ramps, _, _ = driver.execute_query(query)

        # print("Creating curbs query")
        query = "MATCH (s)-[l:`GT/CE-SIDEWALK-CURB`]->(n) RETURN s,n"
        # print(query)
        # print("executing query")
        curbs, _, _ = driver.execute_query(query)

        # print("Creating curbCuts query")
        query = "MATCH (s)-[l:`GT/CE-SIDEWALK-CURB-CUT`]->(n) RETURN s,n"
        # print(query)
        # print("executing query")
        curbCuts, _, _ = driver.execute_query(query)

        # print("Creating crossings query")
        query = "MATCH (s)-[l:`GT/CE-SIDEWALK-CROSSING`]->(n) RETURN s,n"
        # print(query)
        # print("executing query")
        crossings, _, _ = driver.execute_query(query)

        # print("Creating busStops query")
        query = "MATCH (s)-[l:`GT/CE-SIDEWALK-BUS-STOP`]->(n) RETURN s,n"
        # print(query)
        # print("executing query")
        busStops, _, _ = driver.execute_query(query)

        # print("Creating waze query")
        # query = "MATCH (way:`OSM-WAY`)-[r:`WAZE-ALERT`]->(waze:`WAZE-ALERT`) "
        # query += " RETURN way.id as stmAdaPathLinkID,r.__impedance_factor,r.__impedance_effect_type"
        # # print(query)
        # # print("executing query")
        # waze, _, _ = driver.execute_query(query)

        # print("Finished query execution")

    # print("Reading csv:",time())

    factors = pd.read_csv('factors.csv', na_values='NA')

    travelTypes = factors.columns.tolist()[numTravelTypes:]
    speeds = factors[travelTypes].iloc[0]
    ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    ##################################################################
    # Calculate impedance    

    # print("Creating factor tables:",time())

    sidewalk_dict = [record.data() for record in sidewalks]
    sidewalk_df = pd.DataFrame(sidewalk_dict)
    # print(sidewalk_df['ID(na)'])
    sidewalk_df[travelTypes] = pd.DataFrame([sidewalk_df['stmAdaPathLinkLength'].astype('float64').values*3600 for travelType in travelTypes]).T
    sidewalk_df[travelTypes] = sidewalk_df[travelTypes].divide(speeds)
    sidewalk_df['stmAdaPathLinkID'] = sidewalk_df['stmAdaPathLinkID'].astype('int').astype('str')
    # print(sidewalk_df['ID(na)'])
    mul_factors = factors[factors['Impedance Effect Type'] == 'MUL']
    add_factors = factors[factors['Impedance Effect Type'] == 'ADD']

    mul_enumerated_factors = mul_factors[mul_factors['Units'] == 'Enumerated']
    add_enumerated_factors = add_factors[add_factors['Units'] == 'Enumerated']
    mul_nonenumerated_factors = mul_factors[mul_factors['Units'] != 'Enumerated']
    add_nonenumerated_factors = add_factors[add_factors['Units'] != 'Enumerated']

    present_mul_enumerated_factors = mul_enumerated_factors[mul_enumerated_factors['Variable Name'].isin(sidewalk_df.columns)]
    present_add_enumerated_factors = add_enumerated_factors[add_enumerated_factors['Variable Name'].isin(sidewalk_df.columns)]
    present_mul_nonenumerated_factors = mul_nonenumerated_factors[mul_nonenumerated_factors['Variable Name'].isin(sidewalk_df.columns)]
    present_add_nonenumerated_factors = add_nonenumerated_factors[add_nonenumerated_factors['Variable Name'].isin(sidewalk_df.columns)]

    present_mul_nonenumerated_factors = present_mul_nonenumerated_factors.astype({
        'Lower Constraint Bound': 'float64',
        'Upper Constraint Bound': 'float64'})
    present_add_nonenumerated_factors = present_add_nonenumerated_factors.astype({
        'Lower Constraint Bound': 'float64',
        'Upper Constraint Bound': 'float64'})

    # print("Creating waze tables:",time())

    # waze_dict = [record.data() for record in waze]
    # waze_df = pd.DataFrame(waze_dict)
    # if not waze_df.empty:
    #     waze_df[travelTypes] = pd.DataFrame([waze_df['r.__impedance_factor'].values for travelType in travelTypes]).T
    #     mul_waze = waze_df[waze_df['r.__impedance_effect_type'] == 'MUL'].groupby('stmAdaPathLinkID', as_index=False).prod(numeric_only=True)
    #     add_waze = waze_df[waze_df['r.__impedance_effect_type'] == 'ADD'].groupby('stmAdaPathLinkID', as_index=False).sum(numeric_only=True)
    # else:
    #     mul_waze = pd.DataFrame(columns=['stmAdaPathLinkID'])
    #     add_waze = pd.DataFrame(columns=['stmAdaPathLinkID'])

    def apply_factors(row, applyMul, applyWaze):  
        if applyMul:
            filtered_mul_enumerated_factors = present_mul_enumerated_factors[
                present_mul_enumerated_factors['Enumeration'] == row[present_mul_enumerated_factors['Variable Name']].set_axis(present_mul_enumerated_factors['Variable Name'].index)]
            filtered_mul_nonenumerated_factors = present_mul_nonenumerated_factors[
                (present_mul_nonenumerated_factors['Lower Constraint Bound'].astype('float') <= row[present_mul_nonenumerated_factors['Variable Name']].astype('float').set_axis(present_mul_nonenumerated_factors['Variable Name'].index)) &
                (present_mul_nonenumerated_factors['Upper Constraint Bound'].astype('float') >= row[present_mul_nonenumerated_factors['Variable Name']].astype('float').set_axis(present_mul_nonenumerated_factors['Variable Name'].index))]
            row[travelTypes] = row[travelTypes] * filtered_mul_enumerated_factors[travelTypes].prod(axis=0)
            row[travelTypes] = row[travelTypes] * filtered_mul_nonenumerated_factors[travelTypes].prod(axis=0)
            # if applyWaze:
            #     filtered_mul_waze = mul_waze[mul_waze['stmAdaPathLinkID']==row['stmAdaPathLinkID']].reset_index()
            #     if not filtered_mul_waze.empty:
            #         row[travelTypes] = row[travelTypes] * mul_waze[mul_waze['stmAdaPathLinkID']==row['stmAdaPathLinkID']].reset_index().iloc[0][travelTypes]
        
        filtered_add_enumerated_factors = present_add_enumerated_factors[
            present_add_enumerated_factors['Enumeration'] == row[present_add_enumerated_factors['Variable Name']].set_axis(present_add_enumerated_factors['Variable Name'].index)]
        # print(present_add_nonenumerated_factors['Variable Name'])
        # print(row[present_add_nonenumerated_factors['Variable Name']])
        filtered_add_nonenumerated_factors = present_add_nonenumerated_factors[
            (present_add_nonenumerated_factors['Lower Constraint Bound'].astype('float') <= row[present_add_nonenumerated_factors['Variable Name']].astype('float').set_axis(present_add_nonenumerated_factors['Variable Name'].index)) &
            (present_add_nonenumerated_factors['Upper Constraint Bound'].astype('float') >= row[present_add_nonenumerated_factors['Variable Name']].astype('float').set_axis(present_add_nonenumerated_factors['Variable Name'].index))]

        row[travelTypes] = row[travelTypes] + filtered_add_enumerated_factors[travelTypes].sum(axis=0)
        row[travelTypes] = row[travelTypes] + filtered_add_nonenumerated_factors[travelTypes].sum(axis=0)
        # if applyWaze:
        #     filtered_add_waze = add_waze[add_waze['stmAdaPathLinkID']==row['stmAdaPathLinkID']].reset_index()
        #     if not filtered_add_waze.empty:
        #         row[travelTypes] = row[travelTypes] + add_waze[add_waze['stmAdaPathLinkID']==row['stmAdaPathLinkID']].reset_index().iloc[0][travelTypes]

        return row

    # print("Filtering sidewalks:",time())

    sidewalk_df = sidewalk_df.apply(apply_factors, axis='columns', args=(True,True))
    impedance = sidewalk_df
    # print(impedance['ID(na)'])
    # print(impedance.columns)
    for defect in [defects,ramps,curbs,curbCuts,crossings,busStops]:
        
        # print("Filtering attributes:",time())

        additional_attribute_dict = [record.data()['n'] for record in defect]
        if not additional_attribute_dict:
            continue
        additional_attribute_df = pd.DataFrame(additional_attribute_dict)
        additional_attribute_df[travelTypes] = 0.0
        additional_attribute_df['stmAdaPathLinkID'] = additional_attribute_df['stmAssetDefectReportPedLinkID']
        additional_attribute_df['stmAdaPathLinkID'] = additional_attribute_df['stmAdaPathLinkID'].astype('float').fillna(0).astype('int').astype('str')
        
        present_add_enumerated_factors = add_enumerated_factors[add_enumerated_factors['Variable Name'].isin(additional_attribute_df.columns)]
        present_add_nonenumerated_factors = add_nonenumerated_factors[add_nonenumerated_factors['Variable Name'].isin(additional_attribute_df.columns)]
        present_add_nonenumerated_factors = present_add_nonenumerated_factors.astype({
            'Lower Constraint Bound': 'float64',
            'Upper Constraint Bound': 'float64'})
    
    
        additional_attribute_df = additional_attribute_df.apply(apply_factors, axis='columns', args=(False,False))

        impedance = pd.concat([impedance, additional_attribute_df[['stmAdaPathLinkID']+travelTypes]]).groupby(['stmAdaPathLinkID'], as_index=False).sum()

    ##################################################################
    # Format and upload
    
    # print("Creating outputs:",time())
    impedance = impedance.rename(columns={
            'stmAdaPathLinkID':'stmAdaPathLinkID:String(single)',
            'stmAdaPathLinkLength':'stmAdaPathLinkLength:String(single)',
            'ID(na)':'~from',
            'ID(nb)':'~to'
        } | {travelType: travelType+":String(single)" for travelType in travelTypes})

    # print(impedance['~from'])

    impedance = impedance.astype({'stmAdaPathLinkID:String(single)': 'int32'})

    impedance['~id']   = 'bi' + impedance['stmAdaPathLinkID:String(single)'].astype('str') + '-' + impedance['~from'].astype('str') + '-' + impedance['~to'].astype('str')
    impedance['~label'] = 'BASE-IMPEDANCE'
    impedance['__datasetid:String(single)'] = id
    impedance['Timestamp:String(single)'] = ct
    impedance = impedance.drop(impedance[impedance['~id'] == 'bi0-0-0'].index)
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
    
    # Add reverse direction
    cols = list(impedance)
    cols[2], cols[1] = cols[1], cols[2]
    impedanceB_A = impedance.loc[:,cols]
    cols[2], cols[1] = cols[1], cols[2]
    impedanceB_A.columns = cols
    impedanceB_A['~id'] = 'bi' + impedance['stmAdaPathLinkID:String(single)'].astype('str') + '-' + impedanceB_A['~from'].astype('str') + '-' + impedanceB_A['~to'].astype('str')
    impedance = pd.concat([impedance,impedanceB_A])

    impedance.to_csv("/tmp/base_impedance_calculation.csv",index=False)
    s3 = boto3.client('s3')
    s3.upload_file("/tmp/base_impedance_calculation.csv", LOAD_BUCKET, "base_impedance_calculation/base_impedance_calculation.csv")

    bulkLoad_json = {
            "source" : "s3://{}/base_impedance_calculation".format(LOAD_BUCKET),
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

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 
                    'Access-Control-Allow-Headers': 'Content-Type', 
                    'Access-Control-Allow-Origin':'*', 
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
        'body': json.dumps(bulkLoad_response.json())
    }

    