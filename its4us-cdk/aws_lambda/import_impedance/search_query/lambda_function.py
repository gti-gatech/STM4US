import json

from params import find_params
from graph_database_driver import GraphDatabaseDriver


def lambda_handler(event, context):

    # find necessary parameters
    PARAMS = find_params(event)

    # set up locations for new impedance links if needed
    print("Setting up location of the impedance links for search query as needed")
    driverObj = GraphDatabaseDriver(PARAMS)

    # set lat/lon locations for the impedance links as average of the OSM nodes if does not exist
    query = "MATCH (osm1)-[r:`IMPEDANCE`]->(osm2) " \
        "WHERE r.`__lat` IS NULL OR r.`__lon` IS NULL " \
        "SET r.`__lat` = (toFloat(osm1.lat)+toFloat(osm2.lat))/2.0, " \
        "r.`__lon` = (toFloat(osm1.lon)+toFloat(osm2.lon))/2.0"

    print("QUERY:", query)
    driverObj.run_query("EXECUTE", query)

    print("SUCCESS: Done setting up location on impedance links on AWS Neptune database")

    return {
        "statusCode": 200,
        "body": json.dumps("FINISHED: Locations are set for impedance links")
    }
