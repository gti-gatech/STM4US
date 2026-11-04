from constructs import Construct
from aws_cdk import (
    aws_efs as efs,
    aws_ec2 as ec2,
)

class EFSConstruct(Construct):
    """
    Imports existing EFS that contains large Python packages: shapely and pyproj.
    """

    def __init__(
            self, 
            scope: Construct, 
            construct_id: str,
            *,
            config,
            security_group: ec2.ISecurityGroup,
        ):
        
            super().__init__(scope, construct_id)

            # import existing EFS
            self.file_system = efs.FileSystem.from_file_system_attributes(
                self, "ImportedEFS",
                file_system_id=config.efs_id,
                security_group=security_group,
            )

            # import existing access point
            self.efs_access_point = efs.AccessPoint.from_access_point_attributes(
                self, "ImportedAccessPoint",
                access_point_id=config.efs_access_point_id,
                file_system=self.file_system
            )

            
            