from constructs import Construct
from aws_cdk import aws_secretsmanager as secretsmanager


class SecretsConstruct(Construct):
    """References stage-scoped secrets provisioned outside of CDK."""

    def __init__(self, scope: Construct, construct_id: str, *, stage: str):
        super().__init__(scope, construct_id)

        self.admin_token = secretsmanager.Secret.from_secret_name_v2(
            self, "AdminToken", f"its4us/{stage}/admin-token"
        )
        self.client_token = secretsmanager.Secret.from_secret_name_v2(
            self, "ClientToken", f"its4us/{stage}/client-token"
        )
        self.arcadis_api_key = secretsmanager.Secret.from_secret_name_v2(
            self, "ArcadisApiKey", f"its4us/{stage}/arcadis-api-key"
        )
