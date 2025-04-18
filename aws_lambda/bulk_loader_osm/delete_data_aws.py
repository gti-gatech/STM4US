"""
The script contains functions to delete OSM nodes on the AWS Neptune database
based on the input data_set_id.

"""

from neo4j import GraphDatabase, RoutingControl


class OsmAWSDataDelete:

	def __init__(self, data_set_id, QUERY_URL):

		self.data_set_id = data_set_id

		# set up the python driver to send data to graph database
		self.URI = QUERY_URL
		self.AUTH = ("username", "password") # not used

	def delete_nodes(self):

		# delete nodes and the associated links based on the data_set_id as an attribute lookup
		query1 = "MATCH (n:`OSM-NODE` {{__datasetid: '{}'}}) ".format(self.data_set_id)
		query2 = "DETACH DELETE n"
		query = query1 + query2
		self.driver.execute_query(query)

	def delete_ways(self):

		# delete ways and the associated links based on the data_set_id as an attribute lookup
		query1 = "MATCH (n:`OSM-WAY` {{__datasetid: '{}'}}) ".format(self.data_set_id)
		query2 = "DETACH DELETE n"
		query = query1 + query2
		self.driver.execute_query(query)

	def delete_relations(self):

		# delete relations and the associated links based on the data_set_id as an attribute lookup
		query1 = "MATCH (n:`OSM-RELATION` {{__datasetid: '{}'}}) ".format(self.data_set_id)
		query2 = "DETACH DELETE n"
		query = query1 + query2
		self.driver.execute_query(query)

	
	def delete_nodes_data(self):

		# delete OSM nodes based on the input data_set_id
		self.delete_nodes()

		# delete OSM ways based on the input data_set_id
		self.delete_ways()

		# delete OSM relations based on the input data_set_id
		self.delete_relations()

		return


	def create_transaction(self):

		with GraphDatabase.driver(self.URI, auth=self.AUTH, encrypted=True) as self.driver:
			self.delete_nodes_data()

		return