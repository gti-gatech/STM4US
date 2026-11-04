import pandas as pd
from time import time

from graph_database_driver import GraphDatabaseDriver


def query_database(PARAMS, travelTypes):

    print("Querying database:", time())

    IMPEDANCE_LINK_ATTRS = PARAMS["IMPEDANCE_LINK_ATTRS"]
    WAZE_LINK_ATTRS = PARAMS["WAZE_LINK_ATTRS"]
    WAZE_FIELDS = PARAMS["WAZE_FIELDS"]

    driverObj = GraphDatabaseDriver(PARAMS)

    query = "MATCH (na)-[s:`{}`]->(nb) WHERE s.`{}` = '{}' RETURN ". \
        format(PARAMS["BASE_IMPEDANCE_LABEL"], IMPEDANCE_LINK_ATTRS["__datasetid"], 
                PARAMS["DATASETID"])
    
    query += ",".join(["s.`{0}` as `{0}`".format(travelType) for travelType in travelTypes])
    query += ", s.{} as {}, s.{} as {}, ID(na), ID(nb)". \
        format(IMPEDANCE_LINK_ATTRS["stmAdaPathLinkLength"], IMPEDANCE_LINK_ATTRS["stmAdaPathLinkLength"],
                IMPEDANCE_LINK_ATTRS["stmAdaPathLinkID"], IMPEDANCE_LINK_ATTRS["stmAdaPathLinkID"])

    # execute the query
    print("Executing base impedance query: ")
    print(query)
    baseImpedance = driverObj.run_query("CHECK", query)

    if not PARAMS["TEST_WAZE"]:
        
        print("Creating waze query")
        query = "MATCH (way:`{}`)-[r:`{}`]->(waze:`{}`) ". \
            format(PARAMS["OSM_WAY_LABEL"], PARAMS["WAZE_NODE_LABEL"], PARAMS["WAZE_NODE_LABEL"])

        query += " RETURN way.id as {}, r.{} as factor, r.{} as type". \
            format(WAZE_FIELDS["stmAdaPathLinkID"], WAZE_LINK_ATTRS["__impedance_factor"], 
                    WAZE_LINK_ATTRS["__impedance_effect_type"])

        # execute the query
        waze = driverObj.run_query("CHECK", query)

    print("Finished query execution")

    if PARAMS["TEST_WAZE"]:
        print("Testing waze")
        waze_df = pd.read_csv(PARAMS["WAZE_TEST_FILE"], na_values = "NA")
    else:
        waze_dict = [record.data() for record in waze]
        waze_df = pd.DataFrame(waze_dict)

    return baseImpedance, waze_df