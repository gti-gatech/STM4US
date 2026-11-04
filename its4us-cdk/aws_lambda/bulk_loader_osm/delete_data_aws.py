"""
Delete OSM nodes and their related links for a dataset.
"""

from neo4j import GraphDatabase


class OsmAWSDataDelete:
    def __init__(self, PARAMS):
        self.datasetid = PARAMS["DATASETID"]
        self.URI = PARAMS["QUERY_URL"]
        self.osm_fields = PARAMS["OSM_FIELDS"]
        self.osm_node_label = PARAMS["OSM_NODE_LABEL"]
        self.osm_way_label = PARAMS["OSM_WAY_LABEL"]
        self.osm_relation_label = PARAMS["OSM_RELATION_LABEL"]
        self.AUTH = ("username", "password")

    def _delete_label(self, label):
        query = "MATCH (n:`{}` {{{}: $dataset_id}}) DETACH DELETE n".format(
            label, self.osm_fields["__datasetid"]
        )
        self.driver.execute_query(query, parameters_={"dataset_id": self.datasetid})

    def delete_nodes_data(self):
        self._delete_label(self.osm_node_label)
        self._delete_label(self.osm_way_label)
        self._delete_label(self.osm_relation_label)

    def create_transaction(self):
        with GraphDatabase.driver(self.URI, auth=self.AUTH, encrypted=True) as self.driver:
            self.delete_nodes_data()
