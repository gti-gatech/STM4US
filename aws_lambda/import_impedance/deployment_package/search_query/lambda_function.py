import json

from query_writer_setup_locations import ImpedanceLinksLocationSetup


def lambda_handler(event, context):

    env = str(event["env"])

    print("ENV IN SEARCH QUERY LAMBDA:", env)
    LOADER_URL = event['stageVariables']['LOADER_URL']
    QUERY_URL = event['stageVariables']['QUERY_URL']

    # set up locations for new impedance links if needed
    print("Setting up location of the impedance links for search query as needed")
    indexObj = ImpedanceLinksLocationSetup(QUERY_URL)
    indexObj.create_transaction()
    print("Done setting up location on impedance links on AWS Neptune database")

    return {
        "statusCode": 200,
        "body": json.dumps("FINISHED: Locations are set for impedance links")
    }