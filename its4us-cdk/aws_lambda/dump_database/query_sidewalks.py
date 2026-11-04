from graph_database_driver import GraphDatabaseDriver
from cypher_utils import identifier


def query_database(PARAMS):

    driverObj = GraphDatabaseDriver(PARAMS)

    print("Querying sidewalks")
    query = "MATCH (na)-[s:`GT/CE-SIDEWALK`]->(nb) WHERE s.`__datasetid` = $dataset_id RETURN "
    query += "s.stmAdaPathLinkID as stmAdaPathLinkID, s.stmAdaPathLinkLength as stmAdaPathLinkLength,ID(na),ID(nb)"
    sidewalks = driverObj.run_query("CHECK", query, {"dataset_id": PARAMS["DATASETID"]})

    print("Querying defects")
    query = "MATCH (s)-[l:`GT/CE-SIDEWALK-DEFECT`]->(n) RETURN n"
    defects = driverObj.run_query("CHECK", query)

    print("Querying ramps")
    query = "MATCH (s)-[l:`GT/CE-SIDEWALK-RAMP`]->(n) RETURN n"
    ramps = driverObj.run_query("CHECK", query)

    print("Querying curbs")
    query = "MATCH (s)-[l:`GT/CE-SIDEWALK-CURB`]->(n) RETURN n"
    curbs = driverObj.run_query("CHECK", query)

    print("Querying curbCuts")
    query = "MATCH (s)-[l:`GT/CE-SIDEWALK-CURB-CUT`]->(n) RETURN n"
    curbCuts = driverObj.run_query("CHECK", query)

    print("Querying crossings")
    query = "MATCH (s)-[l:`GT/CE-SIDEWALK-CROSSING`]->(n) RETURN n"
    crossings = driverObj.run_query("CHECK", query)

    print("Querying busStops")
    query = "MATCH (s)-[l:`GT/CE-SIDEWALK-BUS-STOP`]->(n) RETURN n"
    busStops = driverObj.run_query("CHECK", query)
    
    print("Querying base impedance")
    query = "MATCH (na)-[s:`BASE-IMPEDANCE`]->(nb) WHERE s.`__datasetid` = $dataset_id RETURN "
    query += ",".join(
        f"s.{identifier(travel_type, set(PARAMS['TRAVEL_TYPES']))} as "
        f"{identifier(travel_type, set(PARAMS['TRAVEL_TYPES']))}"
        for travel_type in PARAMS["TRAVEL_TYPES"]
    )
    query += ",s.stmAdaPathLinkLength as stmAdaPathLinkLength,s.stmAdaPathLinkID as stmAdaPathLinkID,na.id,nb.id"
    baseImpedance = driverObj.run_query(
        "CHECK", query, {"dataset_id": PARAMS["DATASETID"]}
    )
    

    return sidewalks, defects, ramps, curbs, curbCuts, crossings, busStops, baseImpedance
