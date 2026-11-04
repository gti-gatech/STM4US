import logging
from neo4j import GraphDatabase, RoutingControl

logger = logging.getLogger(__name__)


class GraphDatabaseDriver:

    def __init__(self, PARAMS):

        self.URI = PARAMS["QUERY_URL"]
        self.AUTH = ("username", "password") # not used


    def check_existence(self, query):

        # check if a node type already exists in the database or not
        record, _, _ = self.driver.execute_query(
            query,
            routing_=RoutingControl.READ,
        )

        return record

    
    def execute_query(self, query, attrs):

        # execute the query generated
        if attrs:
            self.driver.execute_query(query, attrs=attrs)
        else:
            self.driver.execute_query(query)

        return
    
    
    def run_query(self, action, query, attrs=None):

        record = None

        with GraphDatabase.driver(self.URI, auth=self.AUTH, encrypted=True) as self.driver:

            if action.lower() == "check":

                record = self.check_existence(query)

            elif action.lower() == "execute":

                self.execute_query(query, attrs)

            else:

                message = f"Request action {action} is not supported. Choose CHECK or EXECUTE."
                logger.error(message)
                raise ValueError(message)

        return record