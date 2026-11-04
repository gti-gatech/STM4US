# its4us_cdk/its4us_cdk_stack_pipeline.py
from constructs import Construct

from aws_cdk import (
    Stack,
)

from its4us_cdk.constructs.codepipeline import CodePipelineConstruct

from its4us_cdk.configuration.stage_config import get_config
import its4us_cdk.configuration.constants as constants


class PipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, stage: str, 
                 lambda_gateway_stack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        config = get_config(stage)

        # access resources from lambda stack
        lambda_manager = lambda_gateway_stack.lambda_manager

        # 9. CodePipeline - Run after application Lambdas
        pipeline = CodePipelineConstruct(
            self,
            "CodePipeline",
            config=config,
            lambda_manager=lambda_manager,
            codecommit_repo=config.codecommit_repo_name,
            constants=constants,
        )