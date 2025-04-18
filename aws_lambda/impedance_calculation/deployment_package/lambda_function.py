import json
import requests
import numpy
import pandas as pd
from neo4j import GraphDatabase, RoutingControl
import datetime
import boto3

impedanceBranchKeys = {
    'GT/CE-SIDEWALK-DEFECT': 'DEFECT',
    'GT/CE-SIDEWALK-RAMP': 'RAMP',
    'GT/CE-SIDEWALK-CURB': 'CURB',
    'GT/CE-SIDEWALK-CURB-CUT': 'CUT',
    'GT/CE-SIDEWALK-CROSSING': 'CROSS',
    'GT/CE-SIDEWALK-BUS-STOP': 'BUSS'
}

numTravelTypes = -18

def filter_constraints(row,node,branch):
    if not branch:
        cur_effects = set(str(row['Impedance Effect Type']).split(';'))
        all_effects = impedanceBranchKeys.values()
        if cur_effects.intersection(all_effects):
            return False

    if row['Variable Name'] in node.keys():
        constraints = str(row['Constraints']).split(';')
        lower = row['Lower Bound']
        upper = row['Upper Bound']

        if 'nan' in constraints:
            if (node[row['Variable Name']] == row['Enumeration']):
                print(row['Variable Name'],constraints)
                return True
            else:
                return False
        elif 'INB' in constraints and (float(node[row['Variable Name']]) > lower) and (float(node[row['Variable Name']]) < upper):
            return True
        elif 'INBX' in constraints and (float(node[row['Variable Name']]) >= lower) and (float(node[row['Variable Name']]) <= upper):
            return True
        elif 'OUTB' in constraints and (float(node[row['Variable Name']]) < lower) or (float(node[row['Variable Name']]) > upper):
            return True
        elif 'OUTBX' in constraints and (float(node[row['Variable Name']]) <= lower) or (float(node[row['Variable Name']]) >= upper):
            return True
        else:
            return False
    return False

def lambda_handler(event, context):

    id = event['queryStringParameters']['id']
    
    LOADER_URL = event['stageVariables']['LOADER_URL']
    QUERY_URL = event['stageVariables']['QUERY_URL']
    LOAD_BUCKET = event['stageVariables']['LOAD_BUCKET']
    BULKLOAD_SOURCE = "s3://"+LOAD_BUCKET
    PUBLIC_BUCKET = event['stageVariables']['PUBLIC_BUCKET']

    AUTH = ("username", "password") # not used

    print("Starting driver")
    with GraphDatabase.driver(QUERY_URL, auth=AUTH, encrypted=True) as driver:
        # print("Creating sidewalk query")
        # query = "MATCH (s:`GT/CE-SIDEWALK`)-[:`NODE-A`]->(na), (s)-[:`NODE-B`]->(nb) WHERE s.`__datasetid` = '{}' RETURN s,ID(na),ID(nb)".format(id)
        # print(query)
        # print("executing query")
        # sidewalks, _, _ = driver.execute_query(query)

        # print("Creating branched impedance query")
        # query = "MATCH (s:`GT/CE-SIDEWALK`)-[l]->(n) WHERE n:`GT/CE-SIDEWALK-DEFECT` OR n:`GT/CE-SIDEWALK-RAMP` OR n:`GT/CE-SIDEWALK-CURB` "
        # query += "OR n:`GT/CE-SIDEWALK-CURB-CUT` OR n:`GT/CE-SIDEWALK-CROSSING` OR n:`GT/CE-SIDEWALK-BUS-STOP` RETURN s,n,labels(n)"
        # print(query)
        # print("executing query")
        # impedanceBranched, _, _ = driver.execute_query(query)

        print("Creating branched impedance query")
        query = "MATCH (way:`OSM-WAY`)-[r:`WAZE-ALERT`]->(waze:`WAZE-ALERT`) "
        query += " RETURN way.id,r.__impedance_factor,r.__impedance_effect_type"
        print(query)
        print("executing query")
        waze, _, _ = driver.execute_query(query)
        wayIds = [record.data()['way.id'] for record in waze]
        wayIdsString = json.dumps(wayIds)
        query = "MATCH (na:`OSM-NODE`)-[l:`WAY`]->(nb:`OSM-NODE`) where l.`way-id` in {} ".format(wayIdsString)
        query += " RETURN l.`way-id`,ID(na),ID(nb)"
        print(query)
        print("executing query")
        wazeNodes, _, _ = driver.execute_query(query)

        # print("Creating impedance query")
        # query = "MATCH (a)-[l:IMPEDANCE]->(b) WHERE l.`__datasetid` = '{}' RETURN a".format(id)
        # print(query)
        # print("executing query")
        # impedance, _, _ = driver.execute_query(query)

        print("Finished query execution")

    csv = pd.read_csv('factors.csv', na_values='NA')

    travelTypes = csv.columns.tolist()[numTravelTypes:]
    speeds = csv[travelTypes].iloc[0]

    dataA_B = csv[~csv["Variable Name"].str.contains("B_A", na = True)]
    dataB_A = csv[~csv["Variable Name"].str.contains("A_B", na = True)]
    
    multiplierDataA_B = dataA_B[dataA_B['Impedance Effect Type'].str.contains("MUL", na = False)]
    multiplierDataB_A = dataB_A[dataB_A['Impedance Effect Type'].str.contains("MUL", na = False)]
    addDataA_B = dataA_B[dataA_B['Impedance Effect Type'].str.contains("ADD", na = False)]
    addDataB_A = dataB_A[dataB_A['Impedance Effect Type'].str.contains("ADD", na = False)]
    

    ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # impedanceFile = open("/tmp/impedance_calculation.csv", "w")
    # impedanceFile.write("~id,~from,~to,~label,__datasetid:String(single),Timestamp:String(single),None:String(single),Some:String(single),Device:String(single),WChairM:String(single),WChairE:String(single),MScooter:String(single),Vision:String(single),Vision+:String(single),Some-Vision:String(single),Device-Vision:String(single),WChairM-Vision:String(single),WChairE-Vision:String(single),MScooter-Vision:String(single),Some-Vision+:String(single),Device-Vision+:String(single),WChairM-Vision+:String(single),WChairE-Vision+:String(single),MScooter-Vision+:String(single)\n")

    ## impedanceBranchDict = {
    ##   <sidewalkID>: {
    ##       'GT/CE-SIDEWALK-DEFECT': [node1,node2],
    ##       'GT/CE-SIDEWALK-RAMP': [node3,node4],
    ##       'GT/CE-SIDEWALK-CURB': [],
    ##       'GT/CE-SIDEWALK-CURB-CUT': [node5],
    ##       'GT/CE-SIDEWALK-CROSSING': [],
    ##       'GT/CE-SIDEWALK-BUS-STOP': []
    ##   },
    ##   <sidewalkID>: {...},
    ## }

    # impedanceBranchDict = {}
    # for record in impedanceBranched:
    #     sidewalk = record.data()['s']
    #     if sidewalk['sidewalksimLinkID'] not in impedanceBranchDict:
    #         impedanceBranchDict[sidewalk['sidewalksimLinkID']] = {key:[] for key in impedanceBranchKeys.keys()}
    #     impedanceBranchDict[sidewalk['sidewalksimLinkID']][record.data()['labels(n)'][0]].append(record.data()['n'])

    wazeDict = {
        record.data()['way.id']:{
            'factor':record.data()['r.__impedance_factor'],
            'type':record.data()['r.__impedance_effect_type']
        }
    for record in waze}

    # wazeNodesDict = {
    #     frozenset([record.data()['ID(na)'],record.data()['ID(nb)']]):record.data()['l.way-id']
    # for record in wazeNodes}

    # for record in sidewalks:
    #     sidewalk = record.data()['s']
    #     lengths = pd.Series([sidewalk['stmPedLinkLength'] for type in travelTypes], dtype='float64',index=travelTypes)
    #     baseImpedance = lengths/speeds

    #     # A to B
    #     multipliersA_B = multiplierDataA_B[multiplierDataA_B.apply(filter_constraints,args=(sidewalk,False),axis='columns')]
    #     multipliersA_B = multipliersA_B[travelTypes].prod(axis=0)

    #     # addsA_B = addDataA_B[addDataA_B.apply(filter_constraints,args=(sidewalk,False),axis='columns')]
    #     # addsA_B = addsA_B[travelTypes].sum(axis=0)

    #     # B to A
    #     multipliersB_A = multiplierDataB_A[multiplierDataB_A.apply(filter_constraints,args=(sidewalk,False),axis='columns')]
    #     multipliersB_A = multipliersB_A[travelTypes].prod(axis=0)

    #     # addsB_A = addDataB_A[addDataB_A.apply(filter_constraints,args=(sidewalk,False),axis='columns')]
    #     # addsB_A = addsB_A[travelTypes].sum(axis=0)
        
    #     # multipliersBranchesA_B = pd.DataFrame(columns=travelTypes)
    #     # multipliersBranchesB_A = pd.DataFrame(columns=travelTypes)
    #     addsBranchesA_B = pd.DataFrame(columns=travelTypes)
    #     addsBranchesB_A = pd.DataFrame(columns=travelTypes)

    #     if sidewalk['sidewalksimLinkID'] in impedanceBranchDict:
    #         for branchKey,branchList in impedanceBranchDict[sidewalk['sidewalksimLinkID']].items():
    #             if branchList:
    #                 # multBranchDataA_B = multiplierDataA_B[multiplierDataA_B['Impedance Effect Type'].str.contains(impedanceBranchKeys[branchKey], na = False)]
    #                 # multBranchDataB_A = multiplierDataB_A[multiplierDataB_A['Impedance Effect Type'].str.contains(impedanceBranchKeys[branchKey], na = False)]
    #                 addBranchDataA_B = addDataA_B[addDataA_B['Impedance Effect Type'].str.contains(impedanceBranchKeys[branchKey], na = False)]
    #                 addBranchDataB_A = addDataB_A[addDataB_A['Impedance Effect Type'].str.contains(impedanceBranchKeys[branchKey], na = False)]

    #                 for branch in branchList:
    #                     # constrainedMultBranchDataA_B = multBranchDataA_B[multBranchDataA_B.apply(filter_constraints,args=(branch,True),axis='columns')]
    #                     # constrainedMultBranchDataA_B = constrainedMultBranchDataA_B[travelTypes].prod(axis=0)
    #                     # multipliersBranchesA_B = pd.concat([multipliersBranchesA_B,constrainedMultBranchDataA_B])

    #                     # constrainedMultBranchDataB_A = multBranchDataB_A[multBranchDataB_A.apply(filter_constraints,args=(branch,True),axis='columns')]
    #                     # constrainedMultBranchDataB_A = constrainedMultBranchDataB_A[travelTypes].prod(axis=0)
    #                     # multipliersBranchesB_A = pd.concat([multipliersBranchesB_A,constrainedMultBranchDataB_A])

    #                     constrainedAddsBranchDataA_B = addBranchDataA_B[addBranchDataA_B.apply(filter_constraints,args=(branch,True),axis='columns')]
    #                     constrainedAddsBranchDataA_B = constrainedAddsBranchDataA_B[travelTypes].sum(axis=0)
    #                     addsBranchesA_B = pd.concat([addsBranchesA_B,constrainedAddsBranchDataA_B.to_frame().T],ignore_index=True)

    #                     constrainedAddsBranchDataB_A = addBranchDataB_A[addBranchDataB_A.apply(filter_constraints,args=(branch,True),axis='columns')]
    #                     constrainedAddsBranchDataB_A = constrainedAddsBranchDataB_A[travelTypes].sum(axis=0)
    #                     addsBranchesB_A = pd.concat([addsBranchesB_A,constrainedAddsBranchDataB_A.to_frame().T],ignore_index=True)

    #     # multipliersBranchesA_B = multipliersBranchesA_B[travelTypes].prod(axis=0)
    #     # multipliersBranchesB_A = multipliersBranchesB_A[travelTypes].prod(axis=0)
    #     addsBranchesA_B = addsBranchesA_B[travelTypes].sum(axis=0)
    #     addsBranchesB_A = addsBranchesB_A[travelTypes].sum(axis=0)
    #     # totalImpedanceA_B = baseImpedance*multipliersA_B*multipliersBranchesA_B + addsA_B + addsBranchesA_B
    #     # totalImpedanceB_A = baseImpedance*multipliersB_A*multipliersBranchesB_A + addsB_A + addsBranchesB_A

    #     na = record.data()['ID(na)']
    #     nb = record.data()['ID(nb)']

    #     if frozenset(na,nb) in wazeNodesDict:
    #         impedance = wazeDict[wazeNodesDict[frozenset(na,nb)]]
    #         if impedance['type'] == 'ADD':
    #             addsBranchesA_B += impedance['factor']
    #             addsBranchesB_A += impedance['factor']
    #         elif impedance['type'] == 'MUL':
    #             multipliersA_B *= impedance['factor']
    #             multipliersB_A *= impedance['factor']
        
    #     totalImpedanceA_B = baseImpedance*multipliersA_B + addsBranchesA_B
    #     totalImpedanceB_A = baseImpedance*multipliersB_A + addsBranchesB_A

    #     row = "in{0}-n{1},{0},{1},IMPEDANCE,{2},{3},".format(na,nb,id,ct) + ",".join([string[:6] for string in totalImpedanceA_B.astype(str).tolist()]) + "\n"
    #     impedanceFile.write(row)
    #     row = "in{0}-n{1},{0},{1},IMPEDANCE,{2},{3},".format(nb,na,id,ct) + ",".join([string[:6] for string in totalImpedanceB_A.astype(str).tolist()]) + "\n"
    #     impedanceFile.write(row)

    sidewalks = pd.read_csv('sidewalks.csv', na_values='NA')
    sidewalks['Timestamp:String(single)'] = ct

    dataCols = [
            'None:String(single)',
            'Some:String(single)',
            'Device:String(single)',
            'WChairM:String(single)',
            'WChairE:String(single)',
            'MScooter:String(single)',
            'LowVision:String(single)',
            'Blind:String(single)',
            'Some-LowVision:String(single)',
            'Device-LowVision:String(single)',
            'WChairM-LowVision:String(single)',
            'WChairE-LowVision:String(single)',
            'MScooter-LowVision:String(single)',
            'Some-Blind:String(single)',
            'Device-Blind:String(single)',
            'WChairM-Blind:String(single)',
            'WChairE-Blind:String(single)',
            'MScooter-Blind:String(single)']

    for wayId,impedanceData in wazeDict.items():
        mul = 1
        add = 0
        if impedanceData['type'] == 'ADD':
            add = impedanceData['factor']
        elif impedanceData['type'] == 'MUL':
            mul = impedanceData['factor']
        sidewalks.loc[sidewalks['stmAdaPathLinkID:String(single)'] == int(wayId), dataCols] = sidewalks[dataCols].multiply(mul).add(add)
    
    cols = list(sidewalks)
    cols[2], cols[1] = cols[1], cols[2]
    sidewalksB_A = sidewalks.loc[:,cols]
    cols[2], cols[1] = cols[1], cols[2]
    sidewalksB_A.columns = cols
    sidewalksB_A['~id'] = 'i' + sidewalksB_A['~to'] + '-' + sidewalksB_A['~from']
    
    sidewalks = pd.concat([sidewalks,sidewalksB_A])
    
    sidewalks.to_csv("/tmp/impedance_calculation.csv",index=False)
    
    # impedanceFile.close()
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

    sidewalks = sidewalks[[
        'Timestamp:String(single)',
        '~from',
        '~to',
        'stmAdaPathLinkID:String(single)',
        'stmAdaPathLinkLength:String(single)',
    ] + dataCols]
    
    sidewalks = sidewalks.rename(columns={
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
    
    sidewalks['Upstream Node'] = sidewalks['Upstream Node'].str[1:]
    sidewalks['Downstream Node'] = sidewalks['Downstream Node'].str[1:]
    
    sidewalks.to_csv("/tmp/impedance_export.csv",index=False)
    
    s3.upload_file("/tmp/impedance_export.csv", PUBLIC_BUCKET, "impedance_export.csv")

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 
                    'Access-Control-Allow-Headers': 'Content-Type', 
                    'Access-Control-Allow-Origin':'*', 
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
        'body': json.dumps(bulkLoad_response.json())
    }

    