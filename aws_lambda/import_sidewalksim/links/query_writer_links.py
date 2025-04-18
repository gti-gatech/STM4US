"""
The script consists of openCypher queries for creating/deleting nodes and links for 
SidewalkSim Links data and ingest them to an AWS Neptune database:
	"bolt://<database-name.cluster-id>.us-east-2.neptune.amazonaws.com:8182"

The PUT/POST requests here for the SidewalkSim Links have essentially the same logic and can be used interchangeably.

For more information on the openCypher queries, visit:
	https://neo4j.com/docs/cypher-manual/5/introduction/
	https://neo4j.com/docs/getting-started/cypher-intro/

"""

import boto3
import pandas as pd
from neo4j import GraphDatabase, RoutingControl

from presigned_url_s3 import generate_presigned_url


class SidewalkSimLinksQueries:

	def __init__(self, query_url, method, sidewalk_bucket, s3_path, data_set_id):

		self.method = method # API request method: PUT, POST, DELETE
		self.s3_bucket = sidewalk_bucket
		self.s3_path = s3_path
		self.data_set_id = data_set_id
		self.data = None

		# set up the python driver to send data to graph database
		self.URI = query_url

		print("Neptune database:", self.URI)

		self.AUTH = ("username", "password") # not used

		# create s3 client using boto3
		self.s3_client = boto3.client("s3")
	
	def check_existence(self, query):

		# check if a node type already exists in the database or not
		record, _, _ = self.driver.execute_query(
			query,
			routing_=RoutingControl.READ,
		)

		return record

	def read_object(self):

		# read csv file object from S3 to a pandas dataframe from a presigned url
		print("Generating a presigned URL for S3 object get")
		url = generate_presigned_url(
			self.s3_client, "get_object", {"Bucket": self.s3_bucket, "Key": self.s3_path}, 1000)
		print("Done generating a presigned URL for S3 object get")

		self.data = pd.read_csv(url, sep=",")

	def execute_query(self, query, attrs=None):

		# execute the query generated
		if attrs:
			self.driver.execute_query(query, attrs=attrs)
		else:
			self.driver.execute_query(query)

		return

	def match_node(self, node_id_name, node_id, node_label):

		# write a query to determine if node exists
		query = "MATCH (n:`{}`) WHERE n.`{}` = '{}' RETURN n".format(node_label, node_id_name, node_id)

		#print("MATCH NODE QUERY:", query)

		# check if the node already exists or not
		record = self.check_existence(query)

		#print("MATCH NODE RESULT:", record)

		return record
	
	def match_relationship_id(self, node_id1, node_label1, node_label2, relation_type):

		# write a query to determine if relationship exists between two nodes based on sidewalksimLinkID
		query = "MATCH (sidewalk:`{}`)-[r:`{}`]->(osm:`{}`) WHERE sidewalk.sidewalksimLinkID = '{}' RETURN r".\
			format(node_label1, relation_type, node_label2, node_id1)

		# check if the relationship already exists or not
		record = self.check_existence(query)

		return record
	
	#def match_relationship_attr(self, node_id1, node_label1, node_label2, property2, property2_val):
	#
	#	# write a query to determine if relationship exists between two nodes based on attribute
	#	query = "MATCH (sidewalk:`{}`)-[r]->(osm:`{}`) WHERE sidewalk.sidewalksimLinkID = '{}' AND osm.`{}` = {} RETURN r".\
	#		format(node_label1, node_label2, node_id1, property2, property2_val)
	#
	#	# check if the relationship already exists or not
	#	record = self.check_existence(query)
	#
	#	return record
	
	def delete_node(self, node_id_name, node_id, node_label):

		# delete node and the associated links
		#query1 = "MATCH (n:`{}`) WHERE n.id = '{}' ".format(node_label, node_id)
		query1 = "MATCH (n:`{}`) WHERE n.`{}` = '{}' ".format(node_label, node_id_name, node_id)
		query2 = "DETACH DELETE n"
		query = query1 + query2
		self.execute_query(query)

		return

	def create_node(self, node_label, attrs):

		# create node here
		query = "CREATE (n:`{}` $attrs)".format(node_label)

		#print("CREATE NODE QUERY:", query)

		self.execute_query(query, attrs)

		return

	def update_node(self, node_id, node_label, attrs):

		# update node here
		query = "MATCH (sidewalk:`{}`) WHERE sidewalk.sidewalksimLinkID = '{}' SET sidewalk = $attrs".\
			format(node_label, node_id)

		self.execute_query(query, attrs)

		return
	
	def create_relationship(self, node_id1, node_id2, node_label1, node_label2, relation_type, attrs):
		
		# create relationship between the two nodes; relation_type = NODE-A or NODE-B
		query1 = "MATCH (sidewalk:`{}`), (osm:`{}`) WHERE sidewalk.sidewalksimLinkID = '{}' AND osm.id = '{}' ".\
			format(node_label1, node_label2, node_id1, node_id2)
		
		query2 = "CREATE (sidewalk)-[r:`{}`]->(osm)".format(relation_type)

		query = query1 + query2

		self.execute_query(query, attrs) # attrs contains __datasetid

		return
	
	def remove_property(self, node_id1, node_label1, node_label2, property2_name, property2_val, relation_type):
						    
		# check if a property in a node exists, equals to a value, removes the property if so
		query = "MATCH (sidewalk:`{}`)-[r:`{}`]->(osm:`{}`) WHERE sidewalk.sidewalksimLinkID = '{}' AND osm.`{}` = '{}' REMOVE osm.`{}`".\
			format(node_label1, relation_type, node_label2, node_id1, property2_name, property2_val, property2_name)

		self.execute_query(query)
					 
		return
	
	def set_property_value(self, node_id1, node_label1, node_label2, property2_name, property2_val, 
						property2_name_set, property2_val_set, relation_type):
		
		# set property to a value specified
		query = "MATCH (sidewalk:`{}`)-[r:`{}`]->(osm:`{}`) WHERE sidewalk.sidewalksimLinkID = '{}' AND osm.`{}` = {} SET osm.`{}` = '{}'".\
			format(node_label1, relation_type, node_label2, node_id1, property2_name, property2_val, property2_name_set, property2_val_set)

		self.execute_query(query)

		return 

	def parse_sidewalksim_osm_nodes(self, node_id, nodeAB_id, node_label, relation_type):

		# parse sidewalksimLinkNodeA and sidewalksimLinkNodeB nodes for OSM-NODE nodes
		# determine if node with ID = sidewalksimLinkNodeA or sidewalksimLinkNodeB and label = OSM-NODE exists
		matched = self.match_node("id", nodeAB_id, "OSM-NODE")

		if not matched and not nodeAB_id.startswith("-"): # the OSM-NODE node is not found and its id is positive

			# throw an error message because the node does not exist
			message = "ERROR: A OSM-NODE node with id: {} cannot be found.".format(nodeAB_id)
			print(message)

			return

		if not matched and nodeAB_id.startswith("-"): # the OSM-NODE node is not found and its id is negative

			# the node with id = sidewalksimLinkNodeA/B and label = OSM-NODE does not exist
			# create the node with the id, set __fromsidewalksim to "true", and set __datasetid
			attrs = {}
			attrs["id"] = nodeAB_id
			attrs["__fromsidewalksim"] = "true"
			attrs["__datasetid"] = self.data_set_id
			self.create_node("OSM-NODE", attrs)

		# determine if relationship between GT/CE-SIDEWALK and OSM-NODE exists
		matched_relation_id = self.match_relationship_id(node_id, node_label, "OSM-NODE", relation_type)

		# create the NODE-A/B link if the link does not exist; set __datasetid (OSM-NODE node id is either positive or negative)
		if not matched_relation_id:
			
			attrs = {}
			attrs["__datasetid"] = self.data_set_id
			self.create_relationship(node_id, nodeAB_id, node_label, "OSM-NODE", relation_type, attrs)

		else:

			# the relationship NODE-A/B link exists, the OSM-NODE node is found and its id is positive
			if matched and not nodeAB_id.startswith("-"):

				# set OSM-NODE id to the positive value
				self.set_property_value(node_id, node_label, "OSM-NODE", "__fromsidewalksim", "true", 
					"id", nodeAB_id, relation_type)
				
				# remove __fromsidewalksim = "true" attribute on the OSM-NODE node
				self.remove_property(node_id, node_label, "OSM-NODE", "__fromsidewalksim", "true", relation_type)

		return


	def generate_put_post_query(self):

		# load data in to a pandas dataframe
		self.read_object()

		# parse each row in the data to add or delete nodes/links
		for _, row in self.data.iterrows():

			# extract fields from the data
			node_id = str(row["sidewalksimLinkID"]) # str
			node_label = "GT/CE-SIDEWALK"

			nodeA_id = str(row["sidewalksimLinkNodeA"]) # str
			nodeB_id = str(row["sidewalksimLinkNodeB"]) # str

			# match node with sidewalksimLinkID and label GT/CE-SIDEWALK
			matched = self.match_node("sidewalksimLinkID", node_id, node_label)

			# combine all properties of a node (including id) to a dictionary called attrs
			attrs = row.to_dict()

			# remove sidewalksimLinkNodeA and sidewalksimLinkNodeB keys - will be processed
			attrs.pop("sidewalksimLinkNodeA")
			attrs.pop("sidewalksimLinkNodeB")

			# ensure sidewalksimLinkID is in str
			attrs["sidewalksimLinkID"] = str(attrs["sidewalksimLinkID"])

			# add __datasetid field to attrs
			attrs["__datasetid"] = self.data_set_id

			if matched:

				# update the existing property values for the node
				self.update_node(node_id, node_label, attrs)

			else:

				# create the node with label GT/CE-SIDEWALK and other attributes
				self.create_node(node_label, attrs)

			# parse sidewalksimLinkNodeA and sidewalksimLinkNodeB nodes w.r.t. OSM-NODE nodes
			self.parse_sidewalksim_osm_nodes(node_id, nodeA_id, node_label, "NODE-A")
			self.parse_sidewalksim_osm_nodes(node_id, nodeB_id, node_label, "NODE-B")

		return 
	
	def generate_delete_query(self):

		# delete sidewalksim links nodes and edges based on the __datasetid; label = GT/CE-SIDEWALK
		query1 = "MATCH (n:`GT/CE-SIDEWALK` {{`__datasetid`: '{}'}}) ".format(self.data_set_id)
		query2 = "DETACH DELETE n"
		query = query1 + query2
		self.driver.execute_query(query)

		# delete sidewalksim links nodes and edges based on the __datasetid; label = OSM-NODE
		query1 = "MATCH (n:`OSM-NODE` {{`__datasetid`: '{}'}}) ".format(self.data_set_id)
		query = query1 + query2
		self.driver.execute_query(query)

		return


	def create_transaction(self):

		with GraphDatabase.driver(self.URI, auth=self.AUTH, encrypted=True) as self.driver:

			if self.method == "PUT" or self.method == "POST":
				self.generate_put_post_query()
			elif self.method == "DELETE":
				self.generate_delete_query()
			else:
				message = "ERROR: Request method {} is not supported. Choose PUT, POST or DELETE.".format(self.method)
				print(message)

		return

