from constructs import Construct
from aws_cdk import (
    aws_ec2 as ec2,
)

class NetworkingConstruct(Construct):
    """
    Manages VPC and security groups.
    """
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc_id: str,
        sg_id: str,
        efs_sg_id: str,
        stage: str,
    ):
        super().__init__(scope, construct_id)

        # Import existing VPC
        self.vpc = ec2.Vpc.from_lookup(self, "Vpc", vpc_id=vpc_id)

        # Import security groups
        self.sg = ec2.SecurityGroup.from_security_group_id(self, "SG1", security_group_id=sg_id)

        # Import EFS security group
        self.efs_sg = ec2.SecurityGroup.from_security_group_id(self, "EFSSecurityGroup",
                                                               security_group_id=efs_sg_id,
                                                               )

        # # Create stage-specific security group
        # self.lambda_sg = ec2.SecurityGroup(
        #     self,
        #     f"{stage.capitalize()}LambdaSG",
        #     vpc=self.vpc,
        #     description=f"Security group for {stage} Lambda functions",
        #     allow_all_outbound=True,
        # )

        # # Create Neptune security group
        # self.neptune_sg = ec2.SecurityGroup(
        #     self,
        #     f"{stage.capitalize()}NeptuneSG",
        #     vpc=self.vpc,
        #     description=f"Security group for {stage} Neptune cluster",
        # )

        # # Allow Lambda to talk to Neptune
        # self.neptune_sg.add_ingress_rule(
        #     self.lambda_sg,
        #     ec2.Port.tcp(8182),
        #     "Allow Lambda to connect to Neptune"
        # )