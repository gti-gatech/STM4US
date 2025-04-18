"""
The script consists of openCypher queries for setting up lat and lon of impedance links as average of the two OSM nodes
and using the lat and lon to determine the link locations on an AWS Neptune database:
    "bolt://<database-name.cluster-id>.us-east-2.neptune.amazonaws.com:8182"

"""


from neo4j import GraphDatabase, RoutingControl


class ImpedanceLinksLocationSetup:

    def __init__(self, query_url):
        
        # set up the python driver to send data to graph database
        self.URI = query_url

        print("Neptune database:", self.URI)

        self.AUTH = ("username", "password") # not used


    def check_existence(self, query):

        # check if a node or link type already exists in the database or not
        record, _, _ = self.driver.execute_query(
            query,
            routing_=RoutingControl.READ,
        )

        return record

    
    def execute_query(self, query, attrs=None):

        # execute the query generated
        if attrs:
            self.driver.execute_query(query, attrs=attrs)
        else:
            self.driver.execute_query(query)

        return
    

    def set_average_link_locations(self):

        # set lat/lon locations for the impedance links as average of the OSM nodes if does not exist
        query = "MATCH (osm1)-[r:IMPEDANCE]->(osm2) WHERE r.__lat IS NULL OR r.__lon IS NULL " \
            + "SET r.__lat = (osm1.lat+osm2.lat)/2.0, r.__lon = (osm1.lon+osm2.lon)/2.0"

        self.execute_query(query)

        return 

    
    def generate_location_setup_query(self):

        # create average lat/lon locations for the impedance links
        self.set_average_link_locations()

        return
        
    
    def create_transaction(self):
    
        with GraphDatabase.driver(self.URI, auth=self.AUTH, encrypted=True) as self.driver:

            self.generate_location_setup_query()

        return