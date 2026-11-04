# its4us_cdk/its4us_cdk_stack_lambda_gateway.py
from constructs import Construct

from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_logs as logs,
)

from its4us_cdk.constructs.lambda_manager import LambdaManager
from its4us_cdk.constructs.api_gateway import ApiGatewayConstruct

from its4us_cdk.configuration.stage_config import get_config
import its4us_cdk.configuration.constants as constants


class LambdaGatewayStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, stage: str, 
                 base_stack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        config = get_config(stage)

        # access resources from base stack
        networking = base_stack.networking
        database = base_stack.database
        storage = base_stack.storage
        efs = base_stack.efs
        secrets = base_stack.secrets

        # 5. Authorizer Lambda
        
        # create log group for the lambda function cdk_its4us_auth_{stage}
        log_group = logs.LogGroup(
            self, "LambdaAuthFunctionLogGroup",
            log_group_name=f"/aws/lambda/cdk_its4us_auth_{stage}",  # must match function name
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY # ensure deletion
        )

        self.authorizer_fn = _lambda.Function(
            self,
            "Authorizer",
            function_name=f"cdk_its4us_auth_{stage}",
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("aws_lambda/its4us-auth"),
            memory_size=128,
            timeout=Duration.seconds(5),
            reserved_concurrent_executions=5,
            log_group=log_group,
            environment={
                "ADMIN_TOKEN_SECRET_ID": secrets.admin_token.secret_name,
                "CLIENT_TOKEN_SECRET_ID": secrets.client_token.secret_name,
            },
        )
        secrets.admin_token.grant_read(self.authorizer_fn)
        secrets.client_token.grant_read(self.authorizer_fn)

        # 6. All application Lambdas
        self.lambda_manager = LambdaManager(
            self,
            "Lambdas",
            config=config,
            networking=networking,
            storage=storage,
            database=database,
            efs=efs,
            constants=constants,
            secrets=secrets,
        )

        # 7. API Gateway
        api = ApiGatewayConstruct(
            self,
            "ApiGateway",
            config=config,
            lambda_manager=self.lambda_manager,
            authorizer_function=self.authorizer_fn,
            storage=storage,
        )
