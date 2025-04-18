import json
import requests
from driver import main
from delete_data_aws import OsmAWSDataDelete
import boto3

def lambda_handler(event, context):
    try:
        # xml response from OSM API
        print("ALL EVENT FIELDS:", event)
        
        # find the request method: POST, DELETE
        env = event["requestContext"]["stage"]
        method = event["httpMethod"]
    
        print("ENV:", env)
        print("METHOD USED:", method) # POST, DELETE
        
        LOADER_URL = event['stageVariables']['LOADER_URL']
        QUERY_URL = event['stageVariables']['QUERY_URL']
        LOAD_BUCKET = event['stageVariables']['LOAD_BUCKET']
        BULKLOAD_SOURCE = "s3://"+LOAD_BUCKET
        if method == "POST":
        
            # extract bbox and data_set_id input parameters
            # bbox = event["queryStringParameters"]["bbox"]
            data_set_id = event["queryStringParameters"]["id"]
            
            print("DATA SET ID:", data_set_id)
    
            # # xml response from OSM API
            # osm_response = requests.get('https://api.openstreetmap.org/api/0.6/map', params={'bbox':bbox})
            # with open("/tmp/data.osm", "wb") as binary_file:
            #     binary_file.write(osm_response.content)
    
            # Get complete xml from S3
            s3 = boto3.client('s3')        
            with open("/tmp/data.osm", 'wb') as data:
                s3.download_fileobj(LOAD_BUCKET, "osm-updated.xml", data)
    
            main("/tmp/data.osm", dataset_id=data_set_id)
    
            # s3.upload_file("/tmp/node.csv", LOAD_BUCKET, "osm/node-{}.csv".format(bbox))
            # s3.upload_file("/tmp/way.csv", LOAD_BUCKET, "osm/way-{}.csv".format(bbox))
            # s3.upload_file("/tmp/relation.csv", LOAD_BUCKET, "osm/relation-{}.csv".format(bbox))
            # s3.upload_file("/tmp/wayLink.csv", LOAD_BUCKET, "osm/wayLink-{}.csv".format(bbox))
            # s3.upload_file("/tmp/relationLink.csv", LOAD_BUCKET, "osm/relationLink-{}.csv".format(bbox))
            
            s3.upload_file("/tmp/node.csv", LOAD_BUCKET, "osm-updated/node.csv")
            s3.upload_file("/tmp/way.csv", LOAD_BUCKET, "osm-updated/way.csv")
            s3.upload_file("/tmp/relation.csv", LOAD_BUCKET, "osm-updated/relation.csv")
            s3.upload_file("/tmp/wayLink.csv", LOAD_BUCKET, "osm-updated/wayLink.csv")
            s3.upload_file("/tmp/relationLink.csv", LOAD_BUCKET, "osm-updated/relationLink.csv")

            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 
                        'Access-Control-Allow-Headers': 'Content-Type', 
                        'Access-Control-Allow-Origin':'*', 
                        'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
                'body': json.dumps("FINISHED: Uploaded OSM dataset: {}".format(data_set_id))
            }
    
        if method == "PUT":
    
            bulkLoad_json = {
                "source" : BULKLOAD_SOURCE+"/osm",
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
    
            print(type(bulkLoad_response.json()))
    
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 
                        'Access-Control-Allow-Headers': 'Content-Type', 
                        'Access-Control-Allow-Origin':'*', 
                        'Access-Control-Allow-Methods': 'OPTIONS,PUT,POST,DELETE'},
                'body': json.dumps(bulkLoad_response.json())
            }
        
        if method == "DELETE":
    
            # extract data_set_id input parameter
            data_set_id = event["queryStringParameters"]["id"]
    
            print("DATA SET ID:", data_set_id)
    
            # delete OSM nodes data stored on the AWS Neptune database using data_set_id
            OsmAWSDataDeleteObj = OsmAWSDataDelete(data_set_id, env, QUERY_URL)
            OsmAWSDataDeleteObj.create_transaction()
    
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 
                        'Access-Control-Allow-Headers': 'Content-Type', 
                        'Access-Control-Allow-Origin':'*', 
                        'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
                'body': json.dumps("FINISHED: Deleted dataset: {}".format(data_set_id))
            }
    except Exception as e:
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json', 
                        'Access-Control-Allow-Headers': 'Content-Type', 
                        'Access-Control-Allow-Origin':'*', 
                        'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
                'body': json.dumps(e)
            }