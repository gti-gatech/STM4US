import logging
from graph_database_driver import GraphDatabaseDriver

logger = logging.getLogger(__name__)


def query_sidewalks(PARAMS):

    SIDEWALK_LINK_ATTRS = PARAMS["SIDEWALK_LINK_ATTRS"]

    driverObj = GraphDatabaseDriver(PARAMS)

    if PARAMS["ENV"] == "dev":

        logger.warning("Dev mode: Only first 10 sidewalk data are retrieved to speed up the calculation.")

        # create the queries to execute, limit to 10 only
        query = "MATCH (na)-[s:`{}`]->(nb) WHERE s.`{}` = '{}' RETURN ". \
            format(PARAMS["SIDEWALK_LABEL"], SIDEWALK_LINK_ATTRS["__datasetid"], PARAMS["DATASETID"])
        query += "s.{} as {}, s.{} as {},{},{} LIMIT 10". \
            format(SIDEWALK_LINK_ATTRS["stmAdaPathLinkID"], SIDEWALK_LINK_ATTRS["stmAdaPathLinkID"],
                   SIDEWALK_LINK_ATTRS["stmAdaPathLinkLength"], SIDEWALK_LINK_ATTRS["stmAdaPathLinkLength"],
                   SIDEWALK_LINK_ATTRS["ID(na)"], SIDEWALK_LINK_ATTRS["ID(nb)"])
        sidewalks = driverObj.run_query("CHECK", query)

        query = "MATCH (s)-[l:`{}`]->(n) RETURN s,n LIMIT 10".format(PARAMS["DEFECT_LABEL"])
        defects = driverObj.run_query("CHECK", query)

        query = "MATCH (s)-[l:`{}`]->(n) RETURN s,n LIMIT 10".format(PARAMS["RAMP_LABEL"])
        ramps = driverObj.run_query("CHECK", query)

        query = "MATCH (s)-[l:`{}`]->(n) RETURN s,n LIMIT 10".format(PARAMS["CURB_LABEL"])
        curbs = driverObj.run_query("CHECK", query)

        query = "MATCH (s)-[l:`{}`]->(n) RETURN s,n LIMIT 10".format(PARAMS["CURB_CUT_LABEL"])
        curbCuts = driverObj.run_query("CHECK", query)

        query = "MATCH (s)-[l:`{}`]->(n) RETURN s,n LIMIT 10".format(PARAMS["CROSSING_LABEL"])
        crossings = driverObj.run_query("CHECK", query)

        query = "MATCH (s)-[l:`{}`]->(n) RETURN s,n LIMIT 10".format(PARAMS["BUS_STOP_LABEL"])
        busStops = driverObj.run_query("CHECK", query)

    else:

        # create the queries to execute
        query = "MATCH (na)-[s:`{}`]->(nb) WHERE s.`{}` = '{}' RETURN ". \
            format(PARAMS["SIDEWALK_LABEL"], SIDEWALK_LINK_ATTRS["__datasetid"], PARAMS["DATASETID"])
        query += "s.{} as {}, s.{} as {},{},{}". \
            format(SIDEWALK_LINK_ATTRS["stmAdaPathLinkID"], SIDEWALK_LINK_ATTRS["stmAdaPathLinkID"],
                   SIDEWALK_LINK_ATTRS["stmAdaPathLinkLength"], SIDEWALK_LINK_ATTRS["stmAdaPathLinkLength"],
                   SIDEWALK_LINK_ATTRS["ID(na)"], SIDEWALK_LINK_ATTRS["ID(nb)"])
        sidewalks = driverObj.run_query("CHECK", query)

        query = "MATCH (s)-[l:`{}`]->(n) RETURN s,n".format(PARAMS["DEFECT_LABEL"])
        defects = driverObj.run_query("CHECK", query)

        query = "MATCH (s)-[l:`{}`]->(n) RETURN s,n".format(PARAMS["RAMP_LABEL"])
        ramps = driverObj.run_query("CHECK", query)

        query = "MATCH (s)-[l:`{}`]->(n) RETURN s,n".format(PARAMS["CURB_LABEL"])
        curbs = driverObj.run_query("CHECK", query)

        query = "MATCH (s)-[l:`{}`]->(n) RETURN s,n".format(PARAMS["CURB_CUT_LABEL"])
        curbCuts = driverObj.run_query("CHECK", query)

        query = "MATCH (s)-[l:`{}`]->(n) RETURN s,n".format(PARAMS["CROSSING_LABEL"])
        crossings = driverObj.run_query("CHECK", query)

        query = "MATCH (s)-[l:`{}`]->(n) RETURN s,n".format(PARAMS["BUS_STOP_LABEL"])
        busStops = driverObj.run_query("CHECK", query)


    return sidewalks, defects, ramps, curbs, curbCuts, crossings, busStops