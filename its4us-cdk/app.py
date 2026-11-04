#!/usr/bin/env python3
import os
import aws_cdk as cdk

from its4us_cdk.its4us_cdk_stack_base import BaseStack
from its4us_cdk.its4us_cdk_stack_lambda_gateway import LambdaGatewayStack
from its4us_cdk.its4us_cdk_stack_event import EventStack
from its4us_cdk.its4us_cdk_stack_pipeline import PipelineStack

import stage_pipelines as pipeline_names
from validate_pipeline import PipelineGuard


app = cdk.App()

# ============================ INITIAL CDK STACKS SETUP ============================
# get stage from command line or default to "dev"
stage = app.node.try_get_context("stage") or "dev"

env = cdk.Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), 
                      region=os.getenv("CDK_DEFAULT_REGION"))

# ===================== CODEPIPELINE CHECKS FOR PROD DEPLOYMENT =====================
# check pipeline status (in stage env) if prod deployment
if stage == "prod":
    for pipeline in pipeline_names.PIPELINES:
        guard = PipelineGuard(pipeline)
        guard.validate_prod_deployment(max_age_hours=24)

# ============================ BEGIN CREATING CDK STACKS ============================
# split CDK deployment into multiple stacks - each stack has 500 resources limit
base_stack = BaseStack(
    app, 
    f"Its4us-{stage.capitalize()}-Base", 
    stage=stage, 
    env=env
)

lambda_gateway_stack = LambdaGatewayStack(
    app, 
    f"Its4us-{stage.capitalize()}-Lambda-Gateway",
    stage=stage, 
    base_stack=base_stack, 
    env=env
)

event_stack = EventStack(
    app, 
    f"Its4us-{stage.capitalize()}-Event",
    stage=stage,
    lambda_gateway_stack=lambda_gateway_stack, 
    env=env
)

if stage != "prod":

    # create pipelines for dev and stage only
    pipeline_stack = PipelineStack(
        app, 
        f"Its4us-{stage.capitalize()}-Pipeline",
        stage=stage,
        lambda_gateway_stack=lambda_gateway_stack, 
        env=env
    )


# Its4usCdkStack(app, "Its4us-Dev", stage="dev", env=env)
# Its4usCdkStack(app, "Its4us-Stage", stage="stage", env=env)
# Its4usCdkStack(app, "Its4us-Prod", stage="prod", env=env)

app.synth()
