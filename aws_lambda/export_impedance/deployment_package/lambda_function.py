from query_writer_search_links import ImpedanceLinksSearchQuery
import json

def lambda_handler(event, context):

    # extract env and id parameters
    env = str(event["requestContext"]["stage"])
    QUERY_URL = event['stageVariables']['QUERY_URL']
    id = event["queryStringParameters"]["id"] # search area on the impedance links

    print("ENV:", env)
    print("DATA SET ID OR SEARCH AREA:", id)

    # set impedance csv columns that will be exported
    if 'columns' in event["queryStringParameters"]:
        cols = event["queryStringParameters"]["columns"].split(",")
    else:
        cols = "None,Some,Device,WChairM,WChairE,MScooter,LowVision,Blind," \
        + "Some-LowVision,Device-LowVision,WChairM-LowVision,WChairE-LowVision,MScooter-LowVision," \
        + "Some-Blind,Device-Blind,WChairM-Blind,WChairE-Blind,MScooter-Blind"
        cols = cols.split(",")
    
    # search impedance links in the grid specified by the id
    print("Searching impedance links in the search area:", id)
    indexObj = ImpedanceLinksSearchQuery(cols, id, QUERY_URL)
    out_csv = indexObj.create_transaction()
    print("Done searching impedance links on AWS Neptune database for search area:", id)

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 
                    'Access-Control-Allow-Headers': 'Content-Type', 
                    'Access-Control-Allow-Origin':'*', 
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
        'body': out_csv
    }