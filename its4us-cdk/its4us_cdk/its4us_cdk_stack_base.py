# its4us_cdk/its4us_cdk_stack_base.py
from constructs import Construct

from aws_cdk import (
    Stack,
)

from its4us_cdk.constructs.networking import NetworkingConstruct
from its4us_cdk.constructs.database import DatabaseConstruct
from its4us_cdk.constructs.storage import StorageConstruct
from its4us_cdk.constructs.elastic_file_system import EFSConstruct
from its4us_cdk.constructs.secrets import SecretsConstruct

from its4us_cdk.configuration.stage_config import get_config


class BaseStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, stage: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        config = get_config(stage)
        
        # 1. Networking (VPC + security groups)
        self.networking = NetworkingConstruct(
            self,
            "Networking",
            vpc_id=config.vpc_id,
            sg_id=config.sg_id,
            efs_sg_id=config.efs_sg_id,
            stage=stage,
        )

        # 2. Database (import existing Neptune)
        self.database = DatabaseConstruct(
            self,
            "Database",
            config=config,
            vpc=self.networking.vpc,
            security_group=self.networking.sg,
        )

        # 3. Storage (import existing S3 buckets)
        self.storage = StorageConstruct(
            self,
            "Storage",
            config=config,
        )

        # 4. EFS (import existing EFS)
        self.efs = EFSConstruct(
            self,
            "EFS",
            config=config,
            security_group=self.networking.efs_sg,
        )

        self.secrets = SecretsConstruct(self, "Secrets", stage=stage)
