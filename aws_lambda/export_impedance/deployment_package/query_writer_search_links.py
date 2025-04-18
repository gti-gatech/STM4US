"""
The script consists of openCypher queries for searching and filtering impedance links for a certain grid cell
based on the lat/lon of the links that is set up from query_writer_setup_locations.py on AWS Neptune database:
    dev:
    "bolt://bolt://<database-name.cluster-id>.us-east-2.neptune.amazonaws.com:8182"
    prod:
    "bolt://bolt://<database-name.cluster-id>.us-east-2.neptune.amazonaws.com:8182".

If a certain grid cell is not found from the input, impedance links of whole study area based on __datasetid 
will be found.

"""


from neo4j import GraphDatabase


class ImpedanceLinksSearchQuery:

    def __init__(self, cols, search_area, query_url):

        self.cols = cols # column header of the impedance data to csv
        self.search_area = search_area # coordinate grid name like 33.8N84.3W or search based on __datasetid

        # define the coordinate grids of the study area
        self.study_area = {
            "34.0N84.4W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.4, "max_lon": -84.3},
            "33.8N84.4W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.4, "max_lon": -84.3},
            "33.9N84.4W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.4, "max_lon": -84.3},
            "33.8N84.1W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.1, "max_lon": -84.0},
            "34.0N84.3W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.3, "max_lon": -84.2},
            "33.8N84.3W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.3, "max_lon": -84.2},
            "33.9N84.3W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.3, "max_lon": -84.2},
            "33.8N84.2W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.2, "max_lon": -84.1},
            "33.9N84.2W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.2, "max_lon": -84.1},
            "34.0N84.2W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.2, "max_lon": -84.1},
            "33.9N84.1W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.1, "max_lon": -84.0},
            "34.0N84.1W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.1, "max_lon": -84.0},
            "33.9N84.0W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.0, "max_lon": -83.9},
            "34.0N84.0W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.0, "max_lon": -83.9},
        }
        
        # set up the python driver to send data to graph database
        self.URI = query_url

        print("Neptune database:", self.URI)

        self.AUTH = ("username", "password") # not used

        # set up the initial portion of the output csv file
        self.csv = "Timestamp,Upstream Node,Downstream Node,Way Id,Link Length," + ",".join(self.cols)
        self.csv += "\n"

    
    def search_links_grid(self, search_area):

        results = list()

        # search impedance links in a grid specified based on the lat/lon of the links
        query = "MATCH (osm1)-[r:IMPEDANCE]->(osm2) " \
            + "WHERE osm1.lat >= {} AND osm1.lat <= {} AND osm1.lon >= {} AND osm1.lon <= {} ".\
                format(search_area["min_lat"], search_area["max_lat"], search_area["min_lon"], search_area["max_lon"])

        query += "RETURN r.Timestamp as Timestamp, osm1.id as `Upstream Node`, osm2.id as `Downstream Node`, r.stmAdaPathLinkID as `Way Id`, r.stmAdaPathLinkLength as `Link Length`, " \
            + ', '.join(["r.`{0}` as `{0}`".format(col) for col in self.cols])
        
        # execute the query
        results, _, _ = self.driver.execute_query(query)

        return results
    
    def search_links_full(self, search_area):

        results = list()

        # export impedance links for the full study area based on __datasetid
        query = "MATCH (osm1:`OSM-NODE`)-[r:IMPEDANCE]->(osm2:`OSM-NODE`) WHERE r.__datasetid = '{}' ".format(search_area)

        query += "RETURN r.Timestamp as Timestamp, osm1.id as `Upstream Node`, osm2.id as `Downstream Node`, r.stmAdaPathLinkID as `Way Id`, r.stmAdaPathLinkLength as `Link Length`, " \
            + ', '.join(["r.`{0}` as `{0}`".format(col) for col in self.cols])

        # execute the query
        results, _, _ = self.driver.execute_query(query)

        return results
        
    
    def generate_location_search_query(self):

        try:
            # look up a certain grid cell to search the links
            search_area = self.study_area[self.search_area] # a certain grid cell
            message = "NOTE: Impedance links on grid cell {} will be exported.".format(self.search_area)
            grid_found = True
        except:
            # export the links based on __datasetid of the whole study area
            search_area = self.search_area # whole study area based on __datasetid
            message = "NOTE: Impedance links on full study area with __datasetid: {} will be exported.".format(self.search_area)
            grid_found = False

        print(message)

        if grid_found:
            # search impedance links based on the input grid cell
            results = self.search_links_grid(search_area)
        else:
            # export impedance links based on __datasetid for full study area
            results = self.search_links_full(search_area)

        # save the search query result to csv data format
        for record in results:
            data = record.data()
            self.csv += ",".join(["{}".format(val) for val in data.values()]) + "\n"

        return

    
    def create_transaction(self):
    
        with GraphDatabase.driver(self.URI, auth=self.AUTH, encrypted=True) as self.driver:

            self.generate_location_search_query()

        # return the output csv data format
        out_csv = bytes(self.csv, "utf-8")

        return out_csv