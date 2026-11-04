from constructs import Construct
from aws_cdk import (
    aws_neptune_alpha as neptune,
    aws_ec2 as ec2,
    Stack
)

class DatabaseConstruct(Construct):
    """
    Imports an existing Neptune cluster.
    """
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config,
        vpc: ec2.IVpc,
        security_group: ec2.ISecurityGroup,
    ):
        super().__init__(scope, construct_id)

        self.cluster = neptune.DatabaseCluster.from_database_cluster_attributes(
            self, "ImportedCluster",
            cluster_identifier=config.neptune_cluster_id,
            port=8182,
            security_group=security_group,
            cluster_endpoint_address=config.neptune_endpoint,
            reader_endpoint_address=config.neptune_endpoint.replace(".cluster-", ".cluster-ro-"),
            cluster_resource_identifier=config.neptune_cluster_resource_id
        )
        
        self._endpoint = config.neptune_endpoint


    @property
    def endpoint(self) -> str:
        """Convenience property for the cluster endpoint."""
        return self._endpoint